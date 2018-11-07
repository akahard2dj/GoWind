package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"golang.org/x/text/encoding/korean"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

type TemplateRenderer struct {
	templates *template.Template
}

type Observatory struct {
	ID           uint
	AWSName      string
	AWSLongitude float64
	AWSLatitude  float64
}

type AirPollution struct {
	ID       uint    `json:"id"`
	TagDate  string  `json:"timestamp"`
	ObsName  string  `json:"observatory_name"`
	ItemPM10 float64 `json:"pm10"`
	ItemPM25 float64 `json:"pm25"`
	ItemO3   float64 `json:"o3"`
	ItemNO2  float64 `json:"no2"`
	ItemCO   float64 `json:"co"`
	ItemSO2  float64 `json:"so2"`
}

type WeatherData struct {
	ID                  uint    `json:"id"`
	TagDate             string  `json:"tag_date"`
	ObsName             string  `json:"observatory_name"`
	WindDirection       float64 `json:"wind_direction_value"`
	WindDirectionString string  `json:"wind_direction_str"`
	WindSpeed           float64 `json:"wind_speed"`
	Temperature         float64 `json:"temperature"`
	Precipitation       float64 `json:"precipitation"`
	Humidity            float64 `json:"humidity"`
}

type AirData struct {
	TagDate       string     `json:"timestamp"`
	ObsName       string     `json:"observatory_name"`
	Location      [2]float64 `json:"coordinates"`
	Wind          [2]float64 `json:"wind"`
	Temperature   float64    `json:"temperature"`
	Precipitation float64    `json:"precipitation"`
	Humidity      float64    `json:"humidity"`
	ItemPM10      float64    `json:"pm10"`
	ItemPM25      float64    `json:"pm25"`
	ItemO3        float64    `json:"o3"`
	ItemNO2       float64    `json:"no2"`
	ItemCO        float64    `json:"co"`
	ItemSO2       float64    `json:"so2"`
}

func (t *TemplateRenderer) Render(w io.Writer, dataType string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, dataType, data)
}

func indexPage(c echo.Context) error {
	return c.Render(http.StatusOK, "index.html", map[string]interface{}{
		"data_type": "pm25",
	})
}

func byPass(c echo.Context) error {
	buf, err := ioutil.ReadFile("data/seoul_topo.json")
	if err != nil {
		fmt.Println(err)
	}
	stringSeoulTopo := string(buf)
	return c.String(http.StatusOK, stringSeoulTopo)
}

func redirectPage(c echo.Context) error {
	return c.Render(http.StatusOK, "index.html", map[string]interface{}{
		"data_type": c.Param("data_type"),
	})
}

func getJson(c echo.Context) error {
	db, err := gorm.Open("mysql", "user:passwd@tcp(localhost:3306)/dev_gowind?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	airPollution := []AirPollution{}
	weatherData := []WeatherData{}
	observatory := []Observatory{}

	t := time.Now().Local()

	timeString := fmt.Sprintf("%d-%d-%02d %d:00", t.Year(), t.Month(), t.Day(), t.Hour())

	db.Where("tag_date = ?", timeString).Find(&airPollution)
	db.Where("tag_date = ?", timeString).Find(&weatherData)
	db.Find(&observatory)

	airData := []AirData{}
	for i := 0; i < len(observatory); i++ {
		air := AirData{}
		idxAir := getIndexAirPollution(airPollution, observatory[i].AWSName)
		if idxAir == -1 {
			break
		}
		idxWeather := getIndexWeatherData(weatherData, observatory[i].AWSName)
		if idxWeather == -1 {
			break
		}
		air.TagDate = airPollution[idxAir].TagDate
		air.ObsName = airPollution[idxAir].ObsName
		air.Location[0] = observatory[i].AWSLongitude
		air.Location[1] = observatory[i].AWSLatitude
		air.Wind[0] = weatherData[idxWeather].WindDirection
		air.Wind[1] = weatherData[idxWeather].WindSpeed
		air.Temperature = weatherData[idxWeather].Temperature
		air.Humidity = weatherData[idxWeather].Humidity
		air.Precipitation = weatherData[idxWeather].Precipitation
		air.ItemPM25 = airPollution[idxAir].ItemPM25
		air.ItemPM10 = airPollution[idxAir].ItemPM10
		air.ItemO3 = airPollution[idxAir].ItemO3
		air.ItemNO2 = airPollution[idxAir].ItemNO2
		air.ItemCO = airPollution[idxAir].ItemCO
		air.ItemSO2 = airPollution[idxAir].ItemSO2
		airData = append(airData, air)
	}

	return c.JSON(http.StatusOK, airData)
}

func initObservatory(c echo.Context) error {
	db, err := gorm.Open("mysql", "user:passwd@tcp(localhost:3306)/dev_gowind?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if !db.HasTable(&Observatory{}) {
		db.CreateTable(&Observatory{})
	}

	obsFile, err := os.Open("data/observatory.csv")
	if err != nil {
		fmt.Println(err)
	}
	csvReader := csv.NewReader(bufio.NewReader(obsFile))
	rows, _ := csvReader.ReadAll()
	for i, row := range rows {
		if i != 0 {
			obs := Observatory{}
			obs.AWSName = rows[i][0] + "구"
			valLat, _ := strconv.ParseFloat(rows[i][1], 64)
			obs.AWSLatitude = valLat
			valLon, _ := strconv.ParseFloat(rows[i][2], 64)
			obs.AWSLongitude = valLon

			db.NewRecord(obs)
			db.Create(&obs)
			for j := range row {
				fmt.Printf("%s ", rows[i][j])
			}
			fmt.Println()
		}
	}

	return c.String(http.StatusOK, "Hello, World!")
}

func StringToFloat(strValue string) float64 {
	val, err := strconv.ParseFloat(strValue, 64)
	if err == nil {
		return val
	} else {
		return -999
	}
}

func WeatherDataScrape(c echo.Context) error {
	db, err := gorm.Open("mysql", "user:passwd@tcp(localhost:3306)/dev_gowind?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	resp, err := http.Get("http://aws.seoul.go.kr/RealTime/RealTimeWeatherUser.asp?TITLE=%C0%FC%20%C1%F6%C1%A1%20%C7%F6%C8%B2")
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		log.Fatalf("status code error: %d %s", resp.StatusCode, resp.Status)
	}

	bytes, err := ioutil.ReadAll(resp.Body)
	euckrDecoder := korean.EUCKR.NewDecoder()
	decodedContents, err := euckrDecoder.String(string(bytes))

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(decodedContents))
	if err != nil {
		log.Fatal(err)
	}

	timeItems := strings.Fields(doc.Find(".top tbody tr tbody tr td").Eq(1).Text())
	re := regexp.MustCompile("[0-9]+")

	year := re.FindAllString(timeItems[0], -1)
	month := re.FindAllString(timeItems[1], -1)
	day := re.FindAllString(timeItems[2], -1)
	hour := re.FindAllString(timeItems[3], -1)

	tagDate := fmt.Sprintf("%s-%s-%s %s:00", year[0], month[0], day[0], hour[0])

	queryResult := WeatherData{}
	db.Where("tag_date = ?", tagDate).First(&queryResult)
	var doScrape bool
	if queryResult.ID == 0 {
		doScrape = true
	} else {
		doScrape = false
	}

	if doScrape {
		items := doc.Find(".top .main tr td table tbody tr")
		for i := 1; i < 27; i++ {
			replacedItem := strings.Replace(items.Eq(i).Text(), "\n", "", -1)
			listSubItem := strings.Fields(replacedItem)

			obsName := listSubItem[1] + "구"
			windDirection := StringToFloat(listSubItem[2])
			windDirectionString := listSubItem[3]
			windSpeed := StringToFloat(listSubItem[4])
			temperature := StringToFloat(listSubItem[5])
			precipitation := StringToFloat(listSubItem[6])
			humidity := StringToFloat(listSubItem[8])

			obs := WeatherData{}
			obs.TagDate = tagDate
			obs.ObsName = obsName
			obs.WindDirection = windDirection
			obs.WindDirectionString = windDirectionString
			obs.WindSpeed = windSpeed
			obs.Temperature = temperature
			obs.Precipitation = precipitation
			obs.Humidity = humidity

			db.NewRecord(obs)
			db.Create(&obs)
		}
	}
	return c.String(http.StatusOK, "Hello, World!")
}

func AirPollutionScrape(c echo.Context) error {
	db, err := gorm.Open("mysql", "user:passwd@tcp(localhost:3306)/dev_gowind?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	resp, err := http.Get("http://cleanair.seoul.go.kr/air_city.htm?method=measure&grp1=pm10")
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		log.Fatalf("status code error: %d %s", resp.StatusCode, resp.Status)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	items := doc.Find(".tbl2 tbody tr")
	ii := strings.Fields(strings.Replace(items.Eq(0).Text(), "\n", "", -1))

	tagDate := ii[0] + " " + ii[1]

	queryResult := AirPollution{}
	db.Where("tag_date = ?", tagDate).First(&queryResult)
	var doScrape bool
	if queryResult.ID == 0 {
		doScrape = true
	} else {
		doScrape = false
	}

	if doScrape {
		for i := 1; i < len(items.Nodes); i++ {
			tagTime := items.Eq(i).Find("th").Text()
			subItem := items.Eq(i).Find("td")
			replacedSubItem := strings.Replace(subItem.Text(), "\n", "", -1)
			listSubItem := strings.Fields(replacedSubItem)

			obsName := listSubItem[0]
			pm10 := StringToFloat(listSubItem[1])
			pm25 := StringToFloat(listSubItem[2])
			o3 := StringToFloat(listSubItem[3])
			no2 := StringToFloat(listSubItem[4])
			co := StringToFloat(listSubItem[5])
			so2 := StringToFloat(listSubItem[6])
			obs := AirPollution{}
			obs.ObsName = obsName
			obs.TagDate = tagTime
			obs.ItemPM10 = pm10
			obs.ItemPM25 = pm25
			obs.ItemO3 = o3
			obs.ItemNO2 = no2
			obs.ItemCO = co
			obs.ItemSO2 = so2
			db.NewRecord(obs)
			db.Create(&obs)
		}
	}
	return c.String(http.StatusOK, "Hello, World!")
}

func getIndexAirPollution(data []AirPollution, obsName string) int {
	for i := 0; i < len(data); i++ {
		if data[i].ObsName == obsName {
			if data[i].ItemPM10 == -999 {
				return -1
			}
			if data[i].ItemPM25 == -999 {
				return -1
			}
			if data[i].ItemCO == -999 {
				return -1
			}
			if data[i].ItemNO2 == -999 {
				return -1
			}
			if data[i].ItemSO2 == -999 {
				return -1
			}
			if data[i].ItemO3 == -999 {
				return -1
			}

			return i

		}
	}
	return -1
}

func getIndexWeatherData(data []WeatherData, obsName string) int {
	for i := 0; i < len(data); i++ {
		if data[i].ObsName == obsName {
			return i
		}
	}
	return -1
}

func main() {
	db, err := gorm.Open("mysql", "user:passwd@tcp(localhost:3306)/dev_gowind?charset=utf8")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	renderer := &TemplateRenderer{
		templates: template.Must(template.ParseGlob("templates/*.html")),
	}
	e.Renderer = renderer
	e.Static("/static", "assets")

	e.GET("/", indexPage)
	e.GET("/data/topo_json/", byPass)
	e.GET("/map/current/:data_type", redirectPage)
	e.GET("/data/current/", getJson)
	e.GET("/cronjob/init_obs", initObservatory)
	e.GET("/cronjob/update_airpollution", AirPollutionScrape)
	e.GET("/cronjob/update_weather", WeatherDataScrape)

	e.Logger.Fatal(e.Start(":8000"))

}
