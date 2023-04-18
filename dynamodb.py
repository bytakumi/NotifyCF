import boto3
from boto3.dynamodb.types import (
    TypeSerializer,
    TypeDeserializer
)


class DynamoDB:
    def __init__(self, table_name):
        self.table = boto3.resource("dynamodb").Table(table_name)

    def update_item(self, key, update_expression, expression_attribute_values):
        """ DynamoDBへUpdateItem処理を行う

        Args:
            key (dict): アイテム情報 e.g.) {"id": 1}
            update_expression (string): アップデート内容 e.g.) "SET session_id = :session_id"
            expression_attribute_values (dict): アップデートの値 e.g.) {":session_id": "xxx"}
        """
        self.table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )

    def put_item(self, item):
        """ DynamoDBへPutItem処理を行う

        Args:
            item (dict): アイテム情報 (e.g. {"id": {"N": 1}})
        """
        self.table.put_item(Item=item)

    def get_item(self, key):
        """ DynamoDBへGetItem処理を行う

        Args:
            key (dict): アイテム情報 (e.g. {"id": {"N": 1}})
        """
        return self.table.get_item(Key=key)

    def scan(self, limit):
        """ DynamoDBへScan処理を行う

        Args:
            limit (int): Scanで取得するレコード上限数

        Returns:
            dict: Scan結果
        """
        return self.table.scan(Limit=limit)

    def query(self, condition):
        """ DynamoDBへScan処理を行う

        Args:
            condition (string): query条件

        Returns:
            dict: Query結果
        """
        return self.table.query(KeyConditionExpression=condition)

    def serialize(self, item):
        """ DynamoDBへ渡すアイテム情報をシリアライズする

        e.g.) {"id": 1} → {"id": {"N": 1}}

        Args:
            item (dict): シリアライズ元のアイテム情報

        Returns:
            dict: シリアライズされたアイテム情報
        """
        return {
            k: TypeSerializer().serialize(v)
            for k, v in item.items()
        }

    def deserialize(self, dynamodb_item):
        """DynamoDB形式のJSONからデシリアライズする

        Args:
            dynamodb_item (dict): DynamoDB形式のJSON e.g.) {"id": {"N": 1}}

        Returns:
            (dict): デシリアライズされたJSON e.g.) {"id": 1}
        """
        return {
            k: TypeDeserializer().deserialize(v)
            for k, v in dynamodb_item.items()
        }
