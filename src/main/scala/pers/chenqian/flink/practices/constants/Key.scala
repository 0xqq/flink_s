package pers.chenqian.flink.practices.constants


/**
  *
  */
object Key {

  val Subscribe: String = "subscribe"
  val Kafka: String = "kafka"
  val UTF8 = "utf8"
  val _TEST = "_test"
  val T_UG = "t_ug"
  val T_U = "t_u"
  val T_G = "t_g"
  val EMAIL_LOG_LOCATION = ""
  val REMOVED = "removed"
  val STATUS = "status"
  val RELAY = "relay"
  val TO = "to"
  val FROM = "from"
  val T_GOODS_RAITING = "t_goods_raiting"
  val ID = "id"
  val UID = "uid"
  val STORY_ID = "storyId"
  val BOOTSTRAP_SERVERS_PROP = "bootstrap.servers" //for kafka
  val KAFKA_BOOTSTRAP_SERVERS_PROP = "kafka.bootstrap.servers"
  val GROUP_ID_PROP = "group.id"
  val KEY_DESERIALIZER_PROP = "key.deserializer"
  val VALUE_DESERIALIZER_PROP = "value.deserializer"
  val AUTO_OFFSET_RESET_PROP = "auto.offset.reset"
  val ENABLE_AUTO_COMMIT_PROP = "enable.auto.commit"
  val KAFKA_SEP = ":"
  val GOODS_ID_ = "goods_id"
  val USER_ID_ = "user_id"
  val RAITING = "raiting"
  val VIEW_COUNTS_ = "view_counts"
  val STAY_MS_ = "stay_ms"
  val IS_STAR_ = "is_star"
  val BUY_COUNTS_ = "buy_counts"
  val AT_TIMESTAMP = "@timestamp"
  val YEAR_MONTH_ = "year_month"
  val MYTOPIC = "mytopic"
  val CLICK = "click"
  val GOODS = "goods"
  val GOODSREC = "goodsrec"
  val LATEST = "latest"
  val TRUE = "true"
  val FALSE = "false"
  val REMARK = "remark"
  val CONCERN_SCORE = "concernScore"
  val ES_TYPE = "esType"
  val RECOMMEND = "recommend"
  val CHECK_POINT_DIR_PROP = "check.point.dir"
  val HIVE_WAREHOUSE_PROP = "hive.warehouse"
  val SPARK_SQL_WAREHOUSE_DIR = "spark.sql.warehouse.dir"
  val ES_NODES_PROP = "es.nodes"
  val ES_INDEX_AUTO_CREATE = "es.index.auto.create"
  val NEW_LINE = "\n"
  val SEPARATOR_ = "_"

  //-----  for hbase below  ---------------------------------

  val STATUS_0 = 0
  val FIXED_POOL_SIZE = 50

  //-----  for hbase below  ---------------------------------

  val RECOM_INIT_CAP = 1024


  //-----  for es  ---------------------------------------

  val _INDEX = "_index"
  val _TYPE = "_type"
  val _ID = "_id"
  val CREATE = "create"
  val INDEX = "index" //add a new doc
  val UPDATE = "update"
  val PRESEARCH = "presearch"
  val COMPLETION = "completion"
  val DOC = "doc"
  val INPUT_STR = "input"
  val OUTPUT_STR = "output"
  val PAYLOAD_STR = "payload"
  val WEIGHT_STR = "weight"
  val SUGGEST_WORDS_ = "suggest-words-"
  val IDS = "ids"
  val _MGET = "_mget"
  val _BULK = "_bulk"
  val _SOURCE = "_source"
  val TERM = "term"
  val NUM = "num"
  val MYSQL_BATCH_SIZE = 10000
  val MYSQL_INIT_COLL_SIZE = 15000
  val BATCH_SIZE = 50000
  val INIT_COLL_SIZE = 70000


  //-----  for spark below  ---------------------------------

  val SPARK = "spark"
  val SEARCH = "search"
  val SEARCH_TRIGGER = "searchtrigger"

}
