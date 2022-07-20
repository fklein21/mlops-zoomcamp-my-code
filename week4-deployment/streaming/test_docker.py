import requests

event = {
  "Records": [
    {
      "kinesis": {
        "kinesisSchemaVersion": "1.0",
        "partitionKey": "1",
        "sequenceNumber": "49631414468159203895802538548114255052654779688921595906",
        "data": "eyAicmlkZSI6IHsgIlBVTG9jYXRpb25JRCI6IDEzMCwgIkRPTG9jYXRpb25JRCI6IDIwNSwgInRyaXBfZGlzdGFuY2UiOiAzLjY2IH0sICJyaWRlX2lkIjogMjU2IH0=",
        "approximateArrivalTimestamp": 1657897849.884
      },
      "eventSource": "aws:kinesis",
      "eventVersion": "1.0",
      "eventID": "shardId-000000000000:49631414468159203895802538548114255052654779688921595906",
      "eventName": "aws:kinesis:record",
      "invokeIdentityArn": "arn:aws:iam::318900680121:role/lambda-kinesis-role",
      "awsRegion": "eu-west-1",
      "eventSourceARN": "arn:aws:kinesis:eu-west-1:318900680121:stream/ride_events"
    }
  ]
}

url = 'http://localhost:8080/2015-03-31/functions/function/invocations'

response = requests.post(url, json=event)
print(response.json())