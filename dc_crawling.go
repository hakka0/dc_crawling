package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gocolly/colly"
	"github.com/xuri/excelize/v2"
)

// 데이터 구조체
type PostData struct {
	CollectionTime string
	Nick           string
	UIDIP          string
	PostNum        int
	ComNum         int
	isIP           string
}

type Comment struct {
	UserID  string `json:"user_id"`
	Name    string `json:"name"`
	IP      string `json:"ip"`
	RegDate string `json:"reg_date"`
}

type ResponseData struct {
	Comments []Comment `json:"comments"`
}

var (
	kstLoc       *time.Location
	dataMap      = make(map[string]*PostData)
	mapMutex     sync.Mutex
	sharedClient = &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
)

func init() {
	var err error
	kstLoc, err = time.LoadLocation("Asia/Seoul")
	if err != nil {
		kstLoc = time.FixedZone("KST", 9*60*60)
	}
}

func updateMemory(collectionTime string, nick string, uid string, isPost bool, isIp string) {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	if _, exists := dataMap[uid]; !exists {
		dataMap[uid] = &PostData{
			CollectionTime: collectionTime,
			Nick:           nick,
			UIDIP:          uid,
			isIP:           isIp,
		}
	}
	entry := dataMap[uid]
	if nick != "" {
		entry.Nick = nick
	}
	if isPost {
		entry.PostNum++
	} else {
		entry.ComNum++
	}
}

// [수정] 목록 탐색 함수: 정확도를 위해 동기(Sync) 방식 + 재시도 로직 적용
func findTargetHourPosts(targetStart, targetEnd time.Time) (int, int, string, string) {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)
	c.SetRequestTimeout(30 * time.Second)

	c.OnError(func(r *colly.Response, err error) {
		fmt.Printf("[Error] 페이지 로드 실패 (%s): %v. 재시도합니다.\n", r.Request.URL, err)
		r.Request.Retry()
	})

	var startNo, endNo int
	var startDate, endDate string
	
	page := 1
	done := false
	visitedIDs := make(map[int]bool)

	// 종료 판단용 버퍼 (안전하게 15개로 증가)
	consecutiveOldPosts := 0
	const maxConsecutiveOld = 10

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Referer", "https://gall.dcinside.com/")
	})

	c.OnHTML("tr.ub-content", func(e *colly.HTMLElement) {
		if done { return }

		// 1. 기본 필터링 (공지, AD 등 텍스트 기반)
		numText := e.ChildText("td.gall_num")
		if _, err := strconv.Atoi(numText); err != nil { return } 

		subject := strings.TrimSpace(e.ChildText("td.gall_subject"))
		if subject == "설문" || subject == "AD" || subject == "공지" { return }

		noStr := e.Attr("data-no")
		postNo, err := strconv.Atoi(noStr)
		if err != nil { return }
		
		if visitedIDs[postNo] { return }
		visitedIDs[postNo] = true

		title := e.ChildAttr("td.gall_date", "title")
		if title == "" { title = e.ChildText("td.gall_date") }

		postTime, err := time.ParseInLocation("2006-01-02 15:04:05", title, kstLoc)
		if err != nil { return }

		// [핵심 수정] "함정 카드 발동 무효화"
		// 만약 글 날짜가 타겟 시간보다 '24시간 이상' 과거라면? -> 이건 상단 고정 공지다.
		// 종료 카운트(consecutiveOldPosts)에 포함시키지 말고 그냥 무시(return)한다.
		if targetStart.Sub(postTime) > 24 * time.Hour {
			// fmt.Printf("[Ignore] 너무 오래된 글(공지 추정): %s\n", title) // 디버깅용
			return
		}

		// 2. 타겟 시간 범위 내 (정상 수집)
		if (postTime.Equal(targetStart) || postTime.After(targetStart)) && postTime.Before(targetEnd) {
			consecutiveOldPosts = 0 // 정상 글 찾았으니 초기화
			
			if endNo == 0 || postNo > endNo {
				endNo = postNo
				endDate = title
			}
			if startNo == 0 || postNo < startNo {
				startNo = postNo
				startDate = title
			}
		}

		// 3. 타겟 시간보다 과거 글인 경우 (종료 조건 체크)
		if postTime.Before(targetStart) {
			consecutiveOldPosts++ 
			
			// 15개 연속으로 '진짜 과거 글(어제/오늘 아침 등)'이 나와야 종료
			if consecutiveOldPosts >= maxConsecutiveOld {
				done = true
			}
		} else {
			// 타겟보다 미래(최신) 글이면 리셋
			if postTime.After(targetEnd) || postTime.Equal(targetEnd) {
				consecutiveOldPosts = 0
			}
		}
	})

	for !done {
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/lists/?id=projectmx&page=%d", page)
		// 속도와 차단 방지 균형
		time.Sleep(150 * time.Millisecond)
		c.Visit(pageURL)

		if page > 500 {
			fmt.Println("페이지 탐색 한계 초과")
			break
		}
		page++
	}

	return startNo, endNo, startDate, endDate
}

func scrapePostsAndComments(startNo int, endNo int, collectionTimeStr string) {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
		colly.Async(true),
	)
	c.SetRequestTimeout(60 * time.Second)

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 4,
		Delay:       1 * time.Second,
		RandomDelay: 500 * time.Millisecond,
	})

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Referer", "https://gall.dcinside.com/mgallery/board/lists/?id=projectmx")
	})

	c.OnHTML("div.view_content_wrap", func(e *colly.HTMLElement) {
		noStr := e.Request.URL.Query().Get("no")
		no, err := strconv.Atoi(noStr)
		if err != nil {
			return
		}

		nick := e.ChildAttr(".gall_writer", "data-nick")
		uid := e.ChildAttr(".gall_writer", "data-uid")
		isip := "(반)고닉"
		if uid == "" {
			uid = e.ChildAttr(".gall_writer", "data-ip")
			isip = "유동"
		}

		postDateStr := e.ChildAttr(".gall_date", "title") // 상세페이지 날짜 태그 확인 필요
        if postDateStr == "" { postDateStr = e.ChildText(".gall_date") }
        
        pTime, err := time.ParseInLocation("2006-01-02 15:04:05", postDateStr, kstLoc)
        
        // 글 작성 시간이 타겟 시간 내에 있을 때만 '글 작성 수' 카운트
        if err == nil && (pTime.Equal(targetStart) || pTime.After(targetStart)) && pTime.Before(targetEnd) {
             updateMemory(collectionTimeStr, nick, uid, true, isip)
        } else {
             // 타겟 시간 밖의 글(예: 9시 59분 글)이라면, '글 작성 수'는 올리지 말고 유저 정보만 등록(혹은 무시)
             // 단, 댓글 카운트는 아래 commentSrc에서 챙겨야 하므로 여기선 아무것도 안 함
        }

        esno, _ := e.DOM.Find("input#e_s_n_o").Attr("value")

		updateMemory(collectionTimeStr, nick, uid, true, isip)
		commentSrc(no, esno, collectionTimeStr, targetStart, targetEnd)
	})

	fmt.Printf("[DEBUG] 상세 수집 시작: %d번 ~ %d번 글\n", startNo, endNo)

	for i, no := 0, startNo; no <= endNo; i, no = i+1, no+1 {
		url := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d", no)
		c.Visit(url)
	}
	c.Wait()
}

func commentSrc(no int, esno string, collectionTimeStr string) {
	// 1. esno가 비어있으면(목록에서 못 얻었으면) 상세 페이지에 들어가서 직접 획득 시도
	if esno == "" {
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d&t=cv", no)
		
		// [수정] NewRequest 에러 체크 추가
		req, err := http.NewRequest("GET", pageURL, nil)
		if err != nil {
			return 
		}
		
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Referer", "https://gall.dcinside.com/")
		
		resp, err := sharedClient.Do(req)
		if err == nil {
			// [핵심 수정] goquery 문서 생성 실패 시 nil 체크 (Panic 방지)
			doc, err := goquery.NewDocumentFromReader(resp.Body)
			if err == nil && doc != nil {
				esno, _ = doc.Find("input#e_s_n_o").Attr("value")
			}
			resp.Body.Close()
		}
	}

	// 여전히 esno가 없으면 댓글 수집 불가능하므로 포기
	if esno == "" { return }

	// 2. 댓글 목록 데이터 요청 (POST)
	endpoint := "https://gall.dcinside.com/board/comment/"
	sno := strconv.Itoa(no)
	data := url.Values{}
	data.Set("id", "projectmx")
	data.Set("no", sno)
	data.Set("cmt_id", "projectmx")
	data.Set("cmt_no", sno)
	data.Set("e_s_n_o", esno)
	data.Set("comment_page", "1")
	data.Set("_GALLTYPE_", "M")

	req, err := http.NewRequest("POST", endpoint, strings.NewReader(data.Encode()))
	if err != nil { return }

	headerurl := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d&t=cv", no)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", headerurl)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	resp, err := sharedClient.Do(req)
	if err != nil { return }
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil { return }

	// JSON 파싱 시 빈 응답이거나 형식이 잘못된 경우 방어
	if len(body) == 0 { return }

	var responseData ResponseData
	if err := json.Unmarshal(body, &responseData); err != nil { return }

	for _, comment := range responseData.Comments {
		// [필터링] 댓글돌이 제외
		if strings.TrimSpace(comment.Name) == "댓글돌이" {
			continue
		}

		cTime, err := time.ParseInLocation("2006.01.02 15:04:05", comment.RegDate, kstLoc)
        if err != nil {
             // 포맷이 다를 경우 대비 (하이픈 등)
             cTime, err = time.ParseInLocation("2006-01-02 15:04:05", comment.RegDate, kstLoc)
        }

        if err == nil {
            // [중요] 댓글 시간이 타겟 시간(예: 10:00~11:00) 범위 밖이면 무시
            // 즉, 9시 글을 긁더라도 9시 50분 댓글은 버리고, 10시 02분 댓글만 취함
            if cTime.Before(targetStart) || cTime.After(targetEnd) || cTime.Equal(targetEnd) {
                continue
            }
        }

        cNick := comment.Name
        cUID := comment.UserID
        isip := "(반)고닉"
        
        if cUID == "" {
            cUID = comment.IP
            isip = "유동"
        }
		
		updateMemory(collectionTimeStr, cNick, cUID, false, isip)
	}
}
func saveExcelLocal(filename string) error {
	f := excelize.NewFile()
	sheetName := "Sheet1"
	f.SetSheetName(f.GetSheetName(0), sheetName)

	customColumns := []string{"수집시간", "닉네임", "ID(IP)", "유저타입", "작성글수", "작성댓글수", "총활동수"}

	style, _ := f.NewStyle(&excelize.Style{
		Font: &excelize.Font{Bold: true},
		Fill: excelize.Fill{Type: "pattern", Color: []string{"#E0E0E0"}, Pattern: 1},
	})

	for i, colName := range customColumns {
		cell := fmt.Sprintf("%s%d", string(rune('A'+i)), 1)
		f.SetCellValue(sheetName, cell, colName)
		f.SetCellStyle(sheetName, cell, cell, style)
	}

	rowIndex := 2
	for _, post := range dataMap {
		totalActivity := post.PostNum + post.ComNum

		f.SetCellValue(sheetName, fmt.Sprintf("A%d", rowIndex), post.CollectionTime)
		f.SetCellValue(sheetName, fmt.Sprintf("B%d", rowIndex), post.Nick)
		f.SetCellValue(sheetName, fmt.Sprintf("C%d", rowIndex), post.UIDIP)
		f.SetCellValue(sheetName, fmt.Sprintf("D%d", rowIndex), post.isIP)
		f.SetCellValue(sheetName, fmt.Sprintf("E%d", rowIndex), post.PostNum)
		f.SetCellValue(sheetName, fmt.Sprintf("F%d", rowIndex), post.ComNum)
		f.SetCellValue(sheetName, fmt.Sprintf("G%d", rowIndex), totalActivity)

		rowIndex++
	}

	autoFilterRange := fmt.Sprintf("A1:G%d", rowIndex-1)
	if err := f.AutoFilter(sheetName, autoFilterRange, nil); err != nil {
		return fmt.Errorf("필터 적용 실패: %v", err)
	}

	if err := f.SaveAs(filename); err != nil {
		return fmt.Errorf("엑셀 파일 저장 오류: %v", err)
	}
	fmt.Println("엑셀 파일 생성 완료:", filename)
	return nil
}

func uploadToR2(filename string) error {
	client, bucketName, err := getR2Client()
	if err != nil {
		return err
	}

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("파일 열기 실패: %v", err)
	}
	defer file.Close()

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(filename),
		Body:        file,
		ContentType: aws.String("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
	})

	if err != nil {
		return fmt.Errorf("R2 업로드 실패: %v", err)
	}

	return nil
}

// R2 클라이언트 생성 헬퍼
func getR2Client() (*s3.Client, string, error) {
	accountId := os.Getenv("CF_ACCOUNT_ID")
	accessKeyId := os.Getenv("CF_ACCESS_KEY_ID")
	secretAccessKey := os.Getenv("CF_SECRET_ACCESS_KEY")
	bucketName := os.Getenv("CF_BUCKET_NAME")

	if accountId == "" || accessKeyId == "" || secretAccessKey == "" || bucketName == "" {
		return nil, "", fmt.Errorf("R2 인증 정보 누락")
	}

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountId),
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, secretAccessKey, "")),
		config.WithRegion("auto"),
	)
	if err != nil {
		return nil, "", fmt.Errorf("AWS 설정 로드 실패: %v", err)
	}

	return s3.NewFromConfig(cfg), bucketName, nil
}

// [추가] R2에서 가장 최근 파일의 시간 가져오기
func getLastSavedTime() (time.Time, error) {
	client, bucketName, err := getR2Client()
	if err != nil {
		return time.Time{}, err
	}

	// 파일 목록 가져오기
	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return time.Time{}, err
	}

	var maxTime time.Time

	for _, obj := range output.Contents {
		// 파일명 예시: "2025-12-30_07h.xlsx"
		key := *obj.Key
		if !strings.HasSuffix(key, ".xlsx") {
			continue
		}

		// 확장자 제거 및 날짜 파싱
		datePart := strings.TrimSuffix(key, ".xlsx") // "2025-12-30_07h"
		parsedTime, err := time.ParseInLocation("2006-01-02_15h", datePart, kstLoc)
		if err != nil {
			continue // 파싱 실패 시 건너뜀
		}

		if parsedTime.After(maxTime) {
			maxTime = parsedTime
		}
	}

	return maxTime, nil
}

func forceGC() {
	runtime.GC()
	debug.FreeOSMemory()
}

func main() {
	now := time.Now().In(kstLoc)

	// [핵심 수정 1] "완전히 종료된 시간"의 기준점 설정
	// 예: 현재가 09:50이라면 -> limitTime은 09:00:00
	// 09시 데이터는 10시 00분이 넘어야 수집 대상이 됨
	limitTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, kstLoc)

	// 1. R2에서 마지막으로 저장된 시간 확인
	lastTime, err := getLastSavedTime()

	// 안전장치 및 초기화 로직
	if err != nil || lastTime.IsZero() || time.Since(lastTime) > 24*time.Hour {
		fmt.Println("마지막 기록이 없거나 너무 오래되어, 기본 모드(1시간 전)로 실행합니다.")
		// 마지막 기록이 없으면, 현재 '완료된 시간'의 1시간 전을 마지막 기록으로 가정
		lastTime = limitTime.Add(-1 * time.Hour)
	} else {
		fmt.Printf("마지막 저장된 데이터: %s\n", lastTime.Format("2006-01-02 15시"))
	}

	// [핵심 수정 2] 루프 조건 변경 (now -> limitTime)
	// 예: lastTime=07시, now=09:50 (limit=09:00)
	// 1회차: t=08시. 08시 < 09시 (True) -> 실행 (08:00~08:59 수집)
	// 2회차: t=09시. 09시 < 09시 (False) -> 중단 (09시 데이터는 아직 수집 안 함)
	for t := lastTime.Add(time.Hour); t.Before(limitTime); t = t.Add(time.Hour) {
        // targetStart, targetEnd: 실제 통계를 내고 싶은 시간 (예: 10:00 ~ 11:00)
        targetStart := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, kstLoc)
        targetEnd := targetStart.Add(time.Hour)
        
        // [핵심 수정] 스캔 시작 시간(scanStart)을 타겟보다 1시간 전으로 설정
        // 예: 통계는 10시~11시지만, 글 탐색은 09시~11시 글을 다 뒤진다.
        scanStart := targetStart.Add(-1 * time.Hour) 
        
        collectionTimeStr := targetStart.Format("2006-01-02 15:04")
        filename := fmt.Sprintf("%s_%02dh.xlsx", targetStart.Format("2006-01-02"), targetStart.Hour())

        fmt.Printf(">>> 복구 크롤링 시작: %02d시 통계 (스캔 범위: %02d시~%02d시 글)\n", targetStart.Hour(), scanStart.Hour(), targetStart.Hour())

        dataMap = make(map[string]*PostData)

        // 1. 글 목록 찾기: 범위를 넓게(scanStart ~ targetEnd) 잡아서 9시 글과 10시 글 번호를 다 가져옴
        // 함수 이름은 findTargetHourPosts지만 실제로는 Scan Range를 넣음
        firstPostNo, lastPostNo, _, _ := findTargetHourPosts(scanStart, targetEnd)

		if firstPostNo == 0 || lastPostNo == 0 {
            fmt.Printf("  [SKIP] 게시글 없음\n")
        } else {
			fmt.Printf("  데이터 수집 중... (글 %d ~ %d)\n", firstPostNo, lastPostNo)
			fmt.Printf("  시작 날짜: %s, 마지막 날짜: %s\n", firstPostDa, lastPostDa)
			scrapePostsAndComments(firstPostNo, lastPostNo, collectionTimeStr, targetStart, targetEnd)

			if err := saveExcelLocal(filename); err == nil {
				if err := uploadToR2(filename); err == nil {
					fmt.Printf("  [SUCCESS] %s 업로드 완료\n", filename)
					os.Remove(filename)
				} else {
					log.Printf("  [ERROR] R2 업로드 실패: %v\n", err)
				}
			}
		}

		time.Sleep(3 * time.Second)
		forceGC()
	}

	fmt.Println("모든 작업 완료.")
}
