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
	"sync/atomic"
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
	No         string `json:"no"`
	UserID     string `json:"user_id"`
	Name       string `json:"name"`
	IP         string `json:"ip"`
	RegDate    string `json:"reg_date"`
	GallogIcon string `json:"gallog_icon"`
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

// [목록 탐색 함수] 수정됨: 에러 발생 시 error 반환하여 main에서 중단하도록 함
func findTargetHourPosts(targetStart, targetEnd time.Time) (int, int, string, string, error) {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)
	c.SetRequestTimeout(30 * time.Second)

	c.OnError(func(r *colly.Response, err error) {
		// 단순 로그만 출력하고 재시도
		fmt.Printf("   [List Error] 페이지 로드 실패 (%s): %v. 재시도합니다.\n", r.Request.URL, err)
		r.Request.Retry()
	})

	var startNo, endNo int
	var startDate, endDate string

	page := 1
	done := false
	visitedIDs := make(map[int]bool)

	consecutiveOldPosts := 0
	const maxConsecutiveOld = 15 // 판정 기준 조금 완화

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Referer", "https://gall.dcinside.com/")
	})

	c.OnHTML("tr.ub-content", func(e *colly.HTMLElement) {
		if done { return }

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

		// 타겟 시간보다 24시간 이상 과거 글은 무시 (공지일 확률 높음)
		if targetStart.Sub(postTime) > 24*time.Hour { return }

		// 1. 타겟 범위 내 (정상)
		if (postTime.Equal(targetStart) || postTime.After(targetStart)) && postTime.Before(targetEnd) {
			consecutiveOldPosts = 0
			if endNo == 0 || postNo > endNo {
				endNo = postNo
				endDate = title
			}
			if startNo == 0 || postNo < startNo {
				startNo = postNo
				startDate = title
			}
		}

		// 2. 타겟보다 과거 글 (종료 조건 카운트)
		if postTime.Before(targetStart) {
			consecutiveOldPosts++
			if consecutiveOldPosts >= maxConsecutiveOld {
				done = true
			}
		} else {
			// 타겟보다 미래 글 (아직 도달 안 함)
			if postTime.After(targetEnd) || postTime.Equal(targetEnd) {
				consecutiveOldPosts = 0
			}
		}
	})

	for !done {
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/lists/?id=projectmx&page=%d", page)
		time.Sleep(200 * time.Millisecond) // 딜레이 약간 증가
		c.Visit(pageURL)

		// [핵심 수정] 페이지가 500을 넘어가면 네트워크 오류나 논리 오류로 간주하고 에러 반환
		if page > 500 {
			return 0, 0, "", "", fmt.Errorf("페이지 탐색 한계 초과 (Page > 500). 네트워크 불안정 또는 글을 찾을 수 없음")
		}
		page++
	}

	return startNo, endNo, startDate, endDate, nil
}

func scrapePostsAndComments(startNo int, endNo int, collectionTimeStr string, targetStart, targetEnd time.Time) error {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
		colly.Async(true),
	)
	c.SetRequestTimeout(60 * time.Second)

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 2, // 4 -> 2로 낮춤 (안정성 확보)
		Delay:       2 * time.Second,
		RandomDelay: 1 * time.Second,
	})

	var visitedPosts sync.Map
	var failCount int32

	c.OnError(func(r *colly.Response, err error) {
		retries, _ := strconv.Atoi(r.Ctx.Get("retry_count"))
		if r.StatusCode >= 500 || r.StatusCode == 0 {
			if retries < 3 {
				r.Ctx.Put("retry_count", strconv.Itoa(retries+1))
				r.Request.Retry()
			} else {
				atomic.AddInt32(&failCount, 1)
				fmt.Printf("[FAIL] %s - 3회 재시도 실패. (누적 실패: %d)\n", r.Request.URL, atomic.LoadInt32(&failCount))
			}
		}
	})

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Referer", "https://gall.dcinside.com/mgallery/board/lists/?id=projectmx")
	})

	c.OnHTML("div.view_content_wrap", func(e *colly.HTMLElement) {
		noStr := e.Request.URL.Query().Get("no")
		no, err := strconv.Atoi(noStr)
		if err != nil { return }

		if _, loaded := visitedPosts.LoadOrStore(no, true); loaded { return }

		nick := e.ChildAttr(".gall_writer", "data-nick")
		uid := e.ChildAttr(".gall_writer", "data-uid")

		isip := ""
		if uid == "" {
			uid = e.ChildAttr(".gall_writer", "data-ip")
			isip = "유동"
		} else {
			iconSrc := e.ChildAttr(".gall_writer .writer_nikcon img", "src")
			if iconSrc == "https://nstatic.dcinside.com/dc/w/images/nik.gif" {
				isip = "반고닉"
			} else if iconSrc == "https://nstatic.dcinside.com/dc/w/images/fix_nik.gif" {
				isip = "고닉"
			} else {
				isip = "반고닉"
			}
		}

		postDateStr := e.ChildAttr(".gall_date", "title")
		if postDateStr == "" { postDateStr = e.ChildText(".gall_date") }

		pTime, err := time.ParseInLocation("2006-01-02 15:04:05", postDateStr, kstLoc)

		if err == nil && (pTime.Equal(targetStart) || pTime.After(targetStart)) && pTime.Before(targetEnd) {
			updateMemory(collectionTimeStr, nick, uid, true, isip)
		}

		esno, _ := e.DOM.Find("input#e_s_n_o").Attr("value")
		commentSrc(no, esno, collectionTimeStr, targetStart, targetEnd)
	})

	fmt.Printf("[DEBUG] 상세 수집 시작: %d번 ~ %d번 글\n", startNo, endNo)

	for i, no := 0, startNo; no <= endNo; i, no = i+1, no+1 {
		url := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d", no)
		c.Visit(url)
	}
	c.Wait()

	finalFailCount := atomic.LoadInt32(&failCount)
	// 기준: 실패가 전체의 20% 이상이거나 15개 이상이면 치명적으로 간주
	if finalFailCount > 15 {
		return fmt.Errorf("수집 실패 과다 (실패: %d개) - IP 차단 의심으로 인해 저장을 건너뜁니다", finalFailCount)
	}

	return nil
}

func commentSrc(no int, esno string, collectionTimeStr string, targetStart, targetEnd time.Time) {
	if esno == "" {
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/view/?id=projectmx&no=%d&t=cv", no)
		req, err := http.NewRequest("GET", pageURL, nil)
		if err != nil { return }
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Referer", "https://gall.dcinside.com/")
		resp, err := sharedClient.Do(req)
		if err == nil {
			doc, err := goquery.NewDocumentFromReader(resp.Body)
			if err == nil && doc != nil {
				esno, _ = doc.Find("input#e_s_n_o").Attr("value")
			}
			resp.Body.Close()
		}
	}
	if esno == "" { return }

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
	if len(body) == 0 { return }

	var responseData ResponseData
	if err := json.Unmarshal(body, &responseData); err != nil { return }

	for _, comment := range responseData.Comments {
		if strings.TrimSpace(comment.Name) == "댓글돌이" { continue }

		fullDateStr := fmt.Sprintf("%d.%s", targetStart.Year(), comment.RegDate)
		cTime, err := time.ParseInLocation("2006.01.02 15:04:05", fullDateStr, kstLoc)

		if err == nil {
			if cTime.Before(targetStart) || cTime.After(targetEnd) || cTime.Equal(targetEnd) { continue }
		} else {
			continue
		}

		isip := ""
		uniqueKey := comment.UserID
		if comment.UserID == "" {
			isip = "유동"
			uniqueKey = comment.IP
		} else {
			if strings.Contains(comment.GallogIcon, "fix_nik.gif") {
				isip = "고닉"
			} else {
				isip = "반고닉"
			}
		}
		updateMemory(collectionTimeStr, comment.Name, uniqueKey, false, isip)
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
	if err != nil { return err }

	file, err := os.Open(filename)
	if err != nil { return fmt.Errorf("파일 열기 실패: %v", err) }
	defer file.Close()

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(filename),
		Body:        file,
		ContentType: aws.String("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
	})

	if err != nil { return fmt.Errorf("R2 업로드 실패: %v", err) }
	return nil
}

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
	if err != nil { return nil, "", fmt.Errorf("AWS 설정 로드 실패: %v", err) }

	return s3.NewFromConfig(cfg), bucketName, nil
}

func getLastSavedTime() (time.Time, error) {
	client, bucketName, err := getR2Client()
	if err != nil { return time.Time{}, err }

	output, err := client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})
	if err != nil { return time.Time{}, err }

	var maxTime time.Time
	for _, obj := range output.Contents {
		key := *obj.Key
		if !strings.HasSuffix(key, ".xlsx") { continue }
		datePart := strings.TrimSuffix(key, ".xlsx")
		parsedTime, err := time.ParseInLocation("2006-01-02_15h", datePart, kstLoc)
		if err != nil { continue }
		if parsedTime.After(maxTime) { maxTime = parsedTime }
	}
	return maxTime, nil
}

func forceGC() {
	runtime.GC()
	debug.FreeOSMemory()
}

func main() {
	now := time.Now().In(kstLoc)
	
	// [핵심 수정 1] limitTime 설정 (현재 시간보다 1시간 전까지만 수집하도록)
	// 예: 현재 06:15면, 수집 한계는 05:00 (05:00~05:59 데이터)까지. 06시 데이터는 아직 수집 안 함.
	// limitTime은 "수집을 시작할 수 있는 마지막 시간대 + 1시간" 개념입니다.
	limitTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, kstLoc)

	lastTime, err := getLastSavedTime()

	if err != nil || lastTime.IsZero() || time.Since(lastTime) > 24*time.Hour {
		fmt.Println("마지막 기록이 없거나 너무 오래되어, 기본 모드(1시간 전)로 실행합니다.")
		lastTime = limitTime.Add(-1 * time.Hour)
	} else {
		fmt.Printf("마지막 저장된 데이터: %s\n", lastTime.Format("2006-01-02 15시"))
	}

	// [핵심 수정 2] 반복문 조건 강화 (t.Before(limitTime))
	// t는 수집하려는 시간대의 '시작 시간'입니다.
	// t가 06시라면, 06시 < 06시(limitTime) 은 False이므로 실행되지 않음 -> 미래 수집 방지
	for t := lastTime.Add(time.Hour); t.Before(limitTime); t = t.Add(time.Hour) {
		targetStart := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, kstLoc)
		targetEnd := targetStart.Add(time.Hour)
		scanStart := targetStart.Add(-1 * time.Hour)

		collectionTimeStr := targetStart.Format("2006-01-02 15:04")
		filename := fmt.Sprintf("%s_%02dh.xlsx", targetStart.Format("2006-01-02"), targetStart.Hour())

		fmt.Printf(">>> 복구 크롤링 시작: %02d시 통계\n", targetStart.Hour())

		dataMap = make(map[string]*PostData)

		// [핵심 수정 3] 목록 탐색 중 에러 발생 시 err 반환
		firstPostNo, lastPostNo, firstPostDa, lastPostDa, err := findTargetHourPosts(scanStart, targetEnd)

		if err != nil {
			// [중요] 목록 탐색 실패(페이지 초과 등) 시, 이를 "게시글 없음"으로 넘기지 않고 프로그램 종료
			// 이렇게 해야 다음 실행 시 이 시간대부터 다시 시도함 (데이터 누락 방지)
			fmt.Printf("  [ABORT] %v\n", err)
			fmt.Println("  목록 탐색 중 치명적 오류 발생. 이번 실행을 중단합니다.")
			return 
		}

		if firstPostNo == 0 || lastPostNo == 0 {
			fmt.Printf("  [SKIP] 게시글 없음 (정상)\n")
		} else {
			fmt.Printf("  데이터 수집 중... (글 %d ~ %d)\n", firstPostNo, lastPostNo)
			fmt.Printf("  시작 날짜: %s, 마지막 날짜: %s\n", firstPostDa, lastPostDa)

			err := scrapePostsAndComments(firstPostNo, lastPostNo, collectionTimeStr, targetStart, targetEnd)

			if err != nil {
				fmt.Printf("  [ABORT] %v\n", err)
				fmt.Println("  데이터 무결성을 위해 이번 실행을 무효화하고 종료합니다. 다음 스케줄에 재시도합니다.")
				return
			}

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
