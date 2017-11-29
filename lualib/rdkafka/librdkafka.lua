local librdkafka = require 'rdkafka'

librdkafka.RD_KAFKA_CONF_OK = 0
librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR = 0
librdkafka.RD_KAFKA_PRODUCER = 0
librdkafka.RD_KAFKA_CONSUMER = 1
librdkafka.RD_KAFKA_CONF_OK = 0

librdkafka.RD_KAFKA_OFFSET_BEGINNING = -2
librdkafka.RD_KAFKA_OFFSET_END      = -1  
librdkafka.RD_KAFKA_OFFSET_STORED   = -1000  
librdkafka.RD_KAFKA_OFFSET_INVALID  = -1001

return librdkafka
