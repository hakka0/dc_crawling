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
	// 1. 목록 탐색용 콜렉터는 동기(Sync)로 설정 (Async 제거)
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)

	// 타임아웃 넉넉하게 설정
	c.SetRequestTimeout(30 * time.Second)

	// [중요] 에러 발생 시 재시도 로직
	c.OnError(func(r *colly.Response, err error) {
		fmt.Printf("[Error] 페이지 로드 실패 (%s): %v. 재시도합니다.\n", r.Request.URL, err)
		r.Request.Retry() // 해당 페이지 다시 요청
	})

	var startNo, endNo int
	var startDate, endDate string

	// 동기 방식이므로 Mutex 불필요
	page := 1
	done := false

	// 중복 방문 방지 (게시글 번호 기준)
	visitedIDs := make(map[int]bool)

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Referer", "https://gall.dcinside.com/")
	})

	c.OnHTML("tr.ub-content", func(e *colly.HTMLElement) {
		if done {
			return
		} // 이미 범위를 벗어났으면 파싱 중단

		numText := e.ChildText("td.gall_num")
		if _, err := strconv.Atoi(numText); err != nil {
			return
		} // 공지 등 필터링

		subject := strings.TrimSpace(e.ChildText("td.gall_subject"))
		if subject == "설문" || subject == "AD" || subject == "공지" {
			return
		}

		noStr := e.Attr("data-no")
		postNo, err := strconv.Atoi(noStr)
		if err != nil {
			return
		}

		if visitedIDs[postNo] {
			return
		}
		visitedIDs[postNo] = true

		title := e.ChildAttr("td.gall_date", "title")
		if title == "" {
			title = e.ChildText("td.gall_date")
		}

		// [디버깅] 날짜 파싱 확인
		postTime, err := time.ParseInLocation("2006-01-02 15:04:05", title, kstLoc)
		if err != nil {
			// 날짜 파싱 실패 시 로그 출력 (원인 파악용)
			// fmt.Printf("[Warning] 날짜 파싱 실패 (글번호: %d): %s\n", postNo, title)
			return
		}

		// 1. 타겟 범위 안에 있는 경우 (예: 22:00 ~ 23:00)
		if (postTime.Equal(targetStart) || postTime.After(targetStart)) && postTime.Before(targetEnd) {
			// 가장 최신글(번호가 큰 것)을 endNo로
			if endNo == 0 || postNo > endNo {
				endNo = postNo
				endDate = title
			}
			// 가장 과거글(번호가 작은 것)을 startNo로 -> 계속 갱신됨
			if startNo == 0 || postNo < startNo {
				startNo = postNo
				startDate = title
			}
		}

		// 2. 타겟 범위보다 과거인 경우 (예: 21:59) -> 탐색 종료 신호
		if postTime.Before(targetStart) {
			done = true
		}
	})

	// 동기 방식이므로 for 루프가 단순해짐
	for !done {
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/lists/?id=projectmx&page=%d", page)

		// [속도 조절] 디시 서버 부하 방지 및 차단 회피 (0.2초 대기)
		time.Sleep(200 * time.Millisecond)

		err := c.Visit(pageURL)
		if err != nil {
			// Visit 자체 에러 (OnError에서 처리 안 된 경우)
			fmt.Printf("페이지 방문 치명적 오류: %v\n", err)
			// 여기서 break를 하면 안 되고, 다음 페이지라도 시도하거나 재시도 로직을 믿어야 함
		}

		if page > 500 { // 안전장치: 너무 깊게 들어가지 않도록
			fmt.Println("페이지 탐색 한계 초과 (500페이지)")
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

		esno, _ := e.DOM.Find("input#e_s_n_o").Attr("value")

		updateMemory(collectionTimeStr, nick, uid, true, isip)
		commentSrc(no, esno, collectionTimeStr)
	})

	fmt.Printf("[DEBUG] 상세 수집 시작: %d번 ~ %d번 글\n", startNo, endNo)

	for i, no := 0, startNo; no <= endNo; i, no = i+1, no+1 {
		url := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d", no)
		c.Visit(url)
	}
	c.Wait()
}

func commentSrc(no int, esno string, collectionTimeStr string) {
	if esno == "" {
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d&t=cv", no)
		req, _ := http.NewRequest("GET", pageURL, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0...")
		req.Header.Set("Referer", "https://gall.dcinside.com/")

		resp, err := sharedClient.Do(req)
		if err == nil {
			doc, _ := goquery.NewDocumentFromReader(resp.Body)
			esno, _ = doc.Find("input#e_s_n_o").Attr("value")
			resp.Body.Close()
		}
	}

	if esno == "" {
		return
	}

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
	if err != nil {
		return
	}

	headerurl := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d&t=cv", no)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Referer", headerurl)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	resp, err := sharedClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	var responseData ResponseData
	if err := json.Unmarshal(body, &responseData); err != nil {
		return
	}

	for _, comment := range responseData.Comments {
		_, err := time.ParseInLocation("2006.01.02 15:04:05", comment.RegDate, kstLoc)

		isip := "(반)고닉"
		if err != nil {
		}

		cNick := comment.Name
		if strings.TrimSpace(cNick) == "댓글돌이" {
            continue
        }
		cUID := comment.UserID
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
		targetStart := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, kstLoc)
		targetEnd := targetStart.Add(time.Hour)

		collectionTimeStr := targetStart.Format("2006-01-02 15:04")
		todayDateStr := targetStart.Format("2006-01-02")
		filename := fmt.Sprintf("%s_%02dh.xlsx", todayDateStr, targetStart.Hour())

		fmt.Printf(">>> 복구 크롤링 시작: %02d시 데이터 (%s ~ %s)\n", targetStart.Hour(), targetStart.Format("15:04"), targetEnd.Format("15:04"))

		// 데이터 맵 초기화
		dataMap = make(map[string]*PostData)

		firstPostNo, lastPostNo, firstPostDa, lastPostDa := findTargetHourPosts(targetStart, targetEnd)

		if firstPostNo == 0 || lastPostNo == 0 {
			fmt.Printf("  [SKIP] %02d시: 게시글 없음\n", targetStart.Hour())
		} else {
			fmt.Printf("  데이터 수집 중... (글 %d ~ %d)\n", firstPostNo, lastPostNo)
			fmt.Printf("  시작 날짜: %s, 마지막 날짜: %s\n", firstPostDa, lastPostDa)
			scrapePostsAndComments(firstPostNo, lastPostNo, collectionTimeStr)

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
