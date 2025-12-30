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
	"sort"
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
	kstLoc   *time.Location
	dataMap  = make(map[string]*PostData)
	mapMutex sync.Mutex
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

func findTargetHourPosts(targetStart, targetEnd time.Time) (int, int, string, string) {
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
		colly.Async(true),
	)

	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 2,
		Delay:       1 * time.Second,
	})

	var startNo, endNo int
	var startDate, endDate string
	var listMutex sync.Mutex
	
	page := 1
	verificationPage := false
	done := false
	
	visitedIDs := make(map[int]bool)
	var visitMutex sync.Mutex

	c.OnRequest(func(r *colly.Request) {
		r.Headers.Set("Referer", "https://gall.dcinside.com/")
	})

	c.OnHTML("tr.ub-content", func(e *colly.HTMLElement) {
		numText := e.ChildText("td.gall_num")
		if _, err := strconv.Atoi(numText); err != nil { return }

		subject := strings.TrimSpace(e.ChildText("td.gall_subject"))
		if subject == "설문" || subject == "AD" || subject == "공지" { return }

		noStr := e.Attr("data-no")
		postNo, err := strconv.Atoi(noStr)
		if err != nil { return }
		
		visitMutex.Lock()
		if visitedIDs[postNo] {
			visitMutex.Unlock()
			return
		}
		visitedIDs[postNo] = true
		visitMutex.Unlock()

		title := e.ChildAttr("td.gall_date", "title")
		if title == "" { title = e.ChildText("td.gall_date") }

		postTime, err := time.ParseInLocation("2006-01-02 15:04:05", title, kstLoc)
		if err != nil { return }

		listMutex.Lock()
		defer listMutex.Unlock()

		if (postTime.Equal(targetStart) || postTime.After(targetStart)) && postTime.Before(targetEnd) {
			if endNo == 0 || postNo > endNo {
				endNo = postNo
				endDate = title
			}
			if startNo == 0 || postNo < startNo {
				startNo = postNo
				startDate = title
			}
		}

		if postTime.Before(targetStart) {
			verificationPage = true
		}
	})

	for !done {
		pageURL := fmt.Sprintf("https://gall.dcinside.com/mgallery/board/lists/?id=projectmx&page=%d", page)
		c.Visit(pageURL)
		
		if page % 5 == 0 {
			c.Wait()
			listMutex.Lock()
			if verificationPage { done = true }
			listMutex.Unlock()
		}

		if page > 200 { done = true }
		page++
	}
	c.Wait()

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
		if err != nil { return }

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

	var responseData ResponseData
	if err := json.Unmarshal(body, &responseData); err != nil { return }

	for _, comment := range responseData.Comments {
		_, err := time.ParseInLocation("2006.01.02 15:04:05", comment.RegDate, kstLoc)
		
		isip := "(반)고닉"
		if err != nil { }
		
		cNick := comment.Name
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
		Bucket: aws.String(bucketName),
		Key:    aws.String(filename),
		Body:   file,
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
	
	// 1. R2에서 마지막으로 저장된 시간 확인
	lastTime, err := getLastSavedTime()
	
	// 마지막 기록이 없거나(처음 실행), 너무 오래된 경우(24시간 초과) -> 기본값(1시간 전)만 수행
	// 안전장치: 너무 오래 쉰 경우 수십 시간치를 한 번에 하면 봇 차단/타임아웃 위험
	if err != nil || lastTime.IsZero() || time.Since(lastTime) > 24*time.Hour {
		fmt.Println("마지막 기록이 없거나 너무 오래되어, 기본 모드(1시간 전)로 실행합니다.")
		lastTime = time.Date(now.Year(), now.Month(), now.Day(), now.Hour()-2, 0, 0, 0, kstLoc) 
		// lastTime을 2시간 전으로 설정하면 -> 루프에서 +1시간 하니까 결국 1시간 전 데이터를 긁게 됨
	} else {
		fmt.Printf("마지막 저장된 데이터: %s\n", lastTime.Format("2006-01-02 15시"))
	}

	// 2. 누락된 시간대 복구 루프
	// 예: 마지막이 7시(07h), 현재가 10시(22h) -> 루프: 8시(08h), 9시(09h)
	// targetTime은 lastTime + 1시간부터 시작해서, 현재 시간(now)보다 이전일 때까지 반복
	for t := lastTime.Add(time.Hour); t.Before(now); t = t.Add(time.Hour) {
		// t가 8시라면 -> targetStart: 08:00, targetEnd: 09:00
		// 정각 기준으로 범위를 잡음
		targetStart := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, kstLoc)
		targetEnd := targetStart.Add(time.Hour)
		
		collectionTimeStr := targetStart.Format("2006-01-02 15:04")
		todayDateStr := targetStart.Format("2006-01-02")
		filename := fmt.Sprintf("%s_%02dh.xlsx", todayDateStr, targetStart.Hour())

		fmt.Printf(">>> 복구 크롤링 시작: %02d시 데이터 (%s ~ %s)\n", targetStart.Hour(), targetStart.Format("15:04"), targetEnd.Format("15:04"))

		// 데이터 맵 초기화 (시간대별로 따로 파일 만들어야 하므로)
		dataMap = make(map[string]*PostData)

		firstPostNo, lastPostNo, firstPostDa, lastPostDa := findTargetHourPosts(targetStart, targetEnd)

		if firstPostNo == 0 || lastPostNo == 0 {
			fmt.Printf("  [SKIP] %02d시: 게시글 없음\n", targetStart.Hour())
		} else {
			fmt.Printf("  데이터 수집 중... (글 %d ~ %d)\n", firstPostNo, lastPostNo)
			fmt.Printf("  시작 날짜: %s, 마지막 날짜: %s\n", firstPostDa, lastPostDa)
			scrapePostsAndComments(firstPostNo, lastPostNo, collectionTimeStr)
			
			// 파일 저장 및 업로드
			if err := saveExcelLocal(filename); err == nil {
				if err := uploadToR2(filename); err == nil {
					fmt.Printf("  [SUCCESS] %s 업로드 완료\n", filename)
					os.Remove(filename)
				} else {
					log.Printf("  [ERROR] R2 업로드 실패: %v\n", err)
				}
			}
		}
		
		// 루프 사이 짧은 대기 (봇 차단 방지)
		time.Sleep(3 * time.Second)
		forceGC()
	}
	
	fmt.Println("모든 작업 완료.")
}
