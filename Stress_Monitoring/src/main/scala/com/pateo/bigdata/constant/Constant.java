package com.pateo.bigdata.constant;


    public interface Constant {

        /**
         * Project Configuration Constants
         */
        String JDBC_DRIVER = "jdbc.driver";
        String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
        String JDBC_URL = "jdbc.url";
        String JDBC_USER = "jdbc.user";
        String JDBC_PASSWORD = "jdbc.password";

        String SPARK_SQL_JDBC_URL = "spark.sql.jdbc.url";
        String SPARK_SQL_JDBC_URL_PROD = "spark.sql.jdbc.url.prod";

        String SPARK_LOCAL = "spark.local" ;

        String KAFKA_BROKER_LIST = "metadata.broker.list";
        String KAFKA_TOPICS = "kafka.topics";


        /**
         * Spark Application Constants
         */
        String SPARK_APP_NAME= "getPeriodCount" ;


    }


