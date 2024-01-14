import pandas as pd
import requests
import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import urllib.request
import urllib3
import boto3

# 各種設定
# スプレッドシートのURL
sheet_id = "1ToG7uRymeSZbEJbecd-wvBb3XtXP6t2iXsBAfrwhtd4"
target_sheet = "令和６年能登半島地震"
save_file_name = "社協等リンク集.xlsx"
# URLを持ってくるカラム名
target_columns = ["社協", "ボランティアセンター"]
# AWS S3 bucket
bucket = "volunteer-links"


def handler(event, context):
    # エクセル取得URL
    url = "https://docs.google.com/spreadsheets/d/" + sheet_id + "/export?format=xlsx"
    with urllib.request.urlopen(url) as u:
        with open('/tmp/' + save_file_name, 'bw') as o:
            o.write(u.read())

    # excelを読む
    excel = pd.ExcelFile('/tmp/' + save_file_name)
    df = excel.parse(target_sheet)

    # 時刻
    day = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    df_full = pd.DataFrame(index=[], columns=["団体コード", "都道府県", "市区町村名", "種類", "URL", "全文"])

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
    }
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    for index, row in df.iterrows():

        for c in target_columns:
            url = row[c]

            if pd.isna(url):
                # record = pd.Series([row["団体コード"], row["都道府県"], row["市区町村名"], url, "empty"], index=df_full.columns)
                # df_full = pd.concat([df_full, pd.DataFrame(record).T])
                continue

            url_info = urlparse(url)
            if len(url_info.scheme) == 0:
                record = pd.Series([row["団体コード"], row["都道府県"], row["市区町村名"], c, url, "skip"], index=df_full.columns)
                df_full = pd.concat([df_full, pd.DataFrame(record).T])
                continue

            try:
                res = requests.get(url, timeout=60, headers=headers, verify=False)
            except requests.exceptions.RequestException as e:
                print(e)
                record = pd.Series([row["団体コード"], row["都道府県"], row["市区町村名"], c, url, "error"], index=df_full.columns)
                df_full = pd.concat([df_full, pd.DataFrame(record).T])
                continue

            if res.status_code != 200:
                record = pd.Series(
                    [row["団体コード"], row["都道府県"], row["市区町村名"], c, url, "status_error:" + str(res.status_code)],
                    index=df_full.columns)
                df_full = pd.concat([df_full, pd.DataFrame(record).T])
                continue

            soup = BeautifulSoup(res.content, 'html.parser')
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text()

            lines = []
            for line in text.splitlines():
                line = line.strip()
                for l in line.split("。"):
                    if l == "":
                        continue
                    lines.append(l)

            record = pd.Series([row["団体コード"], row["都道府県"], row["市区町村名"], c, url, "\n".join(lines)], index=df_full.columns)
            df_full = pd.concat([df_full, pd.DataFrame(record).T])

    df_full.to_csv("/tmp/full_text.csv", index=False, encoding="utf_8_sig")
    # shutil.copyfile("full_text.csv", "full_text" + day + ".csv")

    # S3から最新を取る:https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/download_file.html
    s3 = boto3.resource('s3')
    s3.meta.client.download_file(bucket, "full_text.csv", "/tmp/full_text_latest.csv")

    # S3に日付付きをアップロード:https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/upload_file.html
    s3_client = boto3.client('s3')
    s3_client.upload_file("/tmp/full_text.csv", bucket, "full_text" + day + ".csv")

    # 差分確認
    df_old = pd.read_csv("/tmp/full_text.csv")
    df_new = pd.read_csv("/tmp/full_text_latest.csv")
    df_new["update"] = df_new.apply(get_update_flag, axis=1, column="全文", target=df_old)
    df_new[["団体コード", "都道府県", "市区町村名", "種類", "URL", "update"]].to_csv("/tmp/diff_full.csv", encoding="utf_8_sig", index=False)
    df_new["update_text"] = df_new.apply(get_update_text, axis=1, column="全文", target=df_old)
    df_new[["団体コード", "都道府県", "市区町村名", "種類", "URL", "update", "update_text"]].to_csv("/tmp/diff_full_text.csv",
                                                                                    encoding="utf_8_sig",
                                                                                    index=False)
    # S3に各ファイルアップロード
    s3_client.upload_file("/tmp/diff_full_text.csv", bucket, "diff_full_text" + day + ".csv")
    s3_client.put_object(Body=df_new.to_json(orient="records"), Bucket=bucket, Key="diff_full_text" + day + ".json")

    # S3に日付なしをアップロード
    s3_client.upload_file("/tmp/full_text.csv", bucket, "full_text.csv")


def get_update_flag(row, column, target):
    if pd.isna(row["URL"]):
        return "empty"
    if not pd.isna(row[column]):
        if row[column] == "status_error:404":
            return "not found"
        if row[column] == "skip":
            return "skip"
        if row[column] == "error":
            return "error"
    test = target[target["URL"] == row["URL"]]
    if len(test) == 0:
        return "new"
    if pd.isna(test.iloc[0, 5]):  # 5列目という記載方法なのであまり良くない
        return "no data"
    if row[column] == test.iloc[0, 5]:
        return "no update"
    return "updated"


def get_update_text(row, column, target):
    if row["update"] != "updated":
        return ""
    new_text = str(row[column]).split("\n")
    test = target[(target["団体コード"] == row["団体コード"]) & (target["種類"] == row["種類"])]
    if test.empty:
        return "（前回データ取得できず）"
    old_text = str(target[(target["団体コード"] == row["団体コード"]) & (target["種類"] == row["種類"])].iloc[0, 5]).split("\n")

    new_line = []
    for nt in new_text:
        flag = False
        for ot in old_text:
            if nt == ot:
                flag = True
                break
        if not flag:
            new_line.append(nt)

    return "\n".join(new_line)





