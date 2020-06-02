package main

import (
	"encoding/csv"
	"fmt"
	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v3"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var g_linecnt = 0
var g_uniquecnt = 0
var gcfg map[string]interface{}

func main() {
	fmt.Printf("Hello World\n")

	var cmdConvert = &cobra.Command {
		Use:	"convert [src Directory]",
		Short:	"Convert",
		Long:	"Convert",
		Args:	cobra.MinimumNArgs(2),
		Run:	func(cmd *cobra.Command, args []string) {
			fmt.Println("convert:" + strings.Join(args, " "))

			if f, err := os.Stat(args[1]); os.IsNotExist(err) || !f.IsDir() {
				fmt.Println("ディレクトリは存在しません！")
				return
			}

			fconfig, err := os.Open("hello.yml")
			if err != nil {
				fmt.Println("設定ファイルが存在しません。")
				log.Fatal(err)
				return
			}
			defer fconfig.Close()

			d := yaml.NewDecoder(fconfig)
			if err := d.Decode(&gcfg); err != nil {
				log.Fatal(err)
			}
			fmt.Println(gcfg["filter"])

			tstart := time.Now()
			outputfilename := filepath.Join(args[1], tstart.Format("20060102150405")+ ".csv.conv")
			filew, errw := os.OpenFile(outputfilename, os.O_WRONLY|os.O_CREATE, 0666)
			if errw != nil {
				//エラー処理
				log.Fatal(errw)
			}
			defer filew.Close()

			fmt.Println("Start Process: " + tstart.Format("2006/01/02 15:04:05"))
			fmt.Println("NEW FILE:" + outputfilename)

			//ヘッダ生成
			writer := csv.NewWriter(filew)
			writer.Write([]string{"Message ID","送信日時","送信者","件名"})
			writer.Flush()

			//ファイル取得
			filepath.Walk(args[1], func(path string, info os.FileInfo, err error) error {
				// 拡張子がcsvかを確認
				if strings.HasSuffix(path, ".csv") {
					ucnt, linecnt := convertTextEncode(path, writer)
					g_uniquecnt += ucnt
					g_linecnt += linecnt
				}
				return nil
			})

			tend := time.Now()
			elapsed := tend.Sub(tstart)
			fmt.Println("End Process: " + tend.Format("2006/01/02 15:04:05"))
			fmt.Print("UniqueLines:" + strconv.Itoa(g_uniquecnt) + " Lines:" + strconv.Itoa(g_linecnt))
			f := (float64(g_linecnt)/float64(g_linecnt))
			fmt.Println(f)
			fmt.Println(" per:" + strconv.FormatFloat(f, 'f', 2,64))
			fmt.Println(elapsed)
		},
	}

	var rootCmd = &cobra.Command{Use: "app"}
	rootCmd.AddCommand(cmdConvert)
	rootCmd.Execute()
}

func convertTextEncode(path string, writer *csv.Writer) (uniquecnt int, linecnt int) {
	m := make(map[string]int)

	fmt.Print("LOAD FILE:" + path)

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var line []string
	matchcnt := 0

	for {
		line, err = reader.Read()
		if err != nil {
			break
		}
		xi, _ := gcfg["filter"].(string);
		r := regexp.MustCompile(xi)
		if r.MatchString(line[3]) {
			if line[9] == "SMTP_OUT" {
				_, ok := m[line[0]]
				if ok == false {
					//cell1 (Start Date) transform UTC -> JST ('2020/04/07 00:01:10 UTC' -> '2020-04-07 09:01:10')
					t, _ := time.Parse("2006/01/02 15:04:05 UTC", line[1])
					localtime := t.Local()
					//cell5 encoding transform (UTF8->SJIS)
					writer.Write([]string{line[0], localtime.Format("2006/01/02 15:04:05"), line[3], line[5]})
					//writer.Write([]string{line[0], localtime.Format("2006/01/02 15:04:05"), line[3]})

					m[line[0]] = m[line[0]] + 1
					uniquecnt++
				}
			}
			matchcnt++
		}
		linecnt = linecnt + 1
	}
	writer.Flush()

	fmt.Println(" unique:" + strconv.Itoa(uniquecnt) + " match:" + strconv.Itoa(matchcnt) + " read lines:" + strconv.Itoa(linecnt))
	return
}
