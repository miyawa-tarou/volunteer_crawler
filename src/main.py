import shutil
import pandas as pd
import requests
import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import urllib.request
import os
import urllib3

# 手動で比較対象を変える場合はここの文字を変える
old = ""

# エクセル取得URL
url = "https://docs.google.com/spreadsheets/d/1ToG7uRymeSZbEJbecd-wvBb3XtXP6t2iXsBAfrwhtd4/export?format=xlsx"
with urllib.request.urlopen(url) as u:
    with open('社協等リンク集.xlsx', 'bw') as o:
        o.write(u.read())

# URLを持ってくるカラム名
target_columns = ["社協", "ボランティアセンター"]

# excelを読む
excel = pd.ExcelFile("社協等リンク集.xlsx")
df = excel.parse("令和６年能登半島地震")

# 日付周りの処理
day = datetime.datetime.now().strftime("%Y%m%d")

if old == "":
    dir_list = []
    for directory in os.listdir(path="."):
        if os.path.isdir(directory) and not directory.startswith("."):
            dir_list.append(directory)
    # 最新の日付をoldに入れる
    if len(dir_list) > 0:
        dir_list.sort(reverse=True)
        old = dir_list[0]

if day == old:
    new = day + "_"
else:
    new = day

print("比較対象：" + old)
print("新規作成：" + new)

if not os.path.isdir(new):
    os.mkdir(new)

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

        urlInfo = urlparse(url)
        if len(urlInfo.scheme) == 0:
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

df_full.to_csv("full_text.csv", index=False, encoding="utf_8_sig")
shutil.copyfile("full_text.csv", day + "/full_text.csv")

# 初回は取得のみ
if old == "":
    exit(0)


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


df_old = pd.read_csv(old + '/full_text.csv')
df_new = pd.read_csv(new + '/full_text.csv')
df_new["update"] = df_new.apply(get_update_flag, axis=1, column="全文", target=df_old)
df_new[["団体コード", "都道府県", "市区町村名", "種類", "URL", "update"]].to_csv("diff_full.csv", encoding="utf_8_sig", index=False)
df_new["update_text"] = df_new.apply(get_update_text, axis=1, column="全文", target=df_old)
df_new[["団体コード", "都道府県", "市区町村名", "種類", "URL", "update", "update_text"]].to_csv("diff_full_text.csv",
                                                                                encoding="utf_8_sig",
                                                                                index=False)
shutil.copyfile("diff_full_text.csv", new + "/diff_full_text.csv")
