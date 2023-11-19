{
  "type" : "record",
  "name" : "message",
  "namespace" : "ProducerFinnhub",
  "fields" : [ {
    "name" : "data",
    "type" : {
      "type" : "array",
      "items" : {
        "type" : "record",
        "name" : "data",
        "fields" : [ {
          "name" : "c",
          "type":[
            {
               "type":"array",
               "items":["null","string"],
               "default":[]
            },
            "null"
          ],
          "doc" : "Trade conditions"
        }, 
        {
          "name" : "p",
          "type" : "double",
          "doc" : "Price at which the stock was traded"
        }, 
        {
          "name" : "s",
          "type" : "string",
          "doc" : "Symbol of a stock"
        }, 
        {
          "name" : "t",
          "type" : "long",
          "doc" : "Timestamp at which the stock was traded"
        }, 
        {
          "name" : "v",
          "type" : "double",
          "doc" : "Volume at which the stock was traded"
        } ]
      },
      "doc" : "Trades messages"
    },
    "doc"  : "Contains data inside a message"
  }, 
  {
    "name" : "type",
    "type" : "string",
    "doc"  : "Type of message"
  } ],
  "doc" : "A schema for upcoming Finnhub messages"
}


{
	"namespace": "Finnhub.avro",
	"type": "record",
	"name": "trades",
	"fields": [
		{"name": "c", "type": ["array", "null"]},
		{"name": "p",  "type": ["string", "null"]},
		{"name": "s", "type": ["string", "null"]},
		{"name": "t", "type": ["double", "null"]},
		{"name": "v", "type": ["string", "null"]},
		{"name": "order_country_name", "type": ["string", "null"]},
		{"name": "order_city_name", "type": ["string", "null"]},
		{"name": "order_ecommerce_website_name", "type": ["string", "null"]}
	]
}