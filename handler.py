import os
import requests
import re
from dynamodb import DynamoDB
from bs4 import BeautifulSoup
from discordwebhook import Discord


def notify(event, context):
    # コメント一覧を取得する
    url = os.environ["CF_URL"]
    comment_ids = get_id_by_page(url)

    # 未配信のコメントを抽出する
    not_notified_comment_ids = []
    for comment_id in comment_ids:
        if not is_notified_comment(comment_id):
            print(f"未配信メッセージID: {comment_id}")
            not_notified_comment_ids.append(comment_id)

    # Discordへ未配信のコメントを通知する
    if len(not_notified_comment_ids):
        discord = Discord(url=os.environ["DISCORD_WEBHOOK_NOTIFY_COMMENT"])
        discord.post(content=f"新たに{len(not_notified_comment_ids)}件がコメントされました。\nURL: {url}")

    # 未配信のコメントを配信済みにする
    # TODO: 未配信のコメント抽出と同時に処理してもいいかも
    for comment_id in not_notified_comment_ids:
        save_notified_comment(comment_id)

    return {
        "statusCode": 200,
    }

def save_notified_comment(comment_id):
    """配信済みのコメントをテーブルに保存する
    
    Args:
        comment_id(string): コメントのID
    """
    table_name = os.environ["DYNAMODB_COMMENTS_TABLE"]
    dynamodb = DynamoDB(table_name)
    # serialized_key = dynamodb.serialize({"id": comment_id})
    serialized_key = {"id": comment_id}
    item = dynamodb.put_item(serialized_key)
    print(f"[DEBUG] 配信済みのコメントをDBに保存しました：{comment_id}")


def is_notified_comment(comment_id):
    """配信済みのコメントであればTrueを、そうでなければFalseを返す
    
    Args:
        comment_id(string): コメントのID

    Returns:
        (bool): 配信済みのコメントであればTrueを、そうでなければFalse
    """
    table_name = os.environ["DYNAMODB_COMMENTS_TABLE"]
    dynamodb = DynamoDB(table_name)
    # serialized_key = dynamodb.serialize({"id": comment_id})
    serialized_key = {"id": comment_id}
    item = dynamodb.get_item(serialized_key)
    return bool(item.get("Item"))


def get_id_by_page(url):
    """ページからコメントのIDを取得し返す

    Args:
        url(string): コメントのIDを取得する先のURL

    Returns:
        (list): コメントのIDのリスト
    """
    res = requests.get(url)
    bs4 = BeautifulSoup(res.content, "html.parser")
    comments = bs4.find_all("span", id=re.compile("^cf-comment(-reply)?-\w+"))
    comment_ids = [comment.attrs.get("id") for comment in comments]
    return comment_ids
 