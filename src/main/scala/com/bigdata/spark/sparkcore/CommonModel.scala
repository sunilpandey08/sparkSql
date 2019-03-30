//package com.bigdata.spark.sparkcore
//
//import org.apache.spark.sql.DataFrame
//import scala.util.Failure
//import scala.util.Success
//import scala.util.Try
//
//class CommonModel {
//
//  val NoTag: Nothing = null
//  def getMetastoreAsDataFrame(commonModel: CommonModel): DataFrame = {
//    var metaStoreDF = commonModel.sparkSession.emptyDataFrame;
//    Try {
//      metaStoreDF = commonModel.sparkSession.read.format(CommonConstants.jdbcFormat)
//        .option(CommonConstants.dbUrl, commonModel.url)
//        .option(CommonConstants.dbTable, commonModel.table)
//        .option(CommonConstants.dbUser, commonModel.user)
//        .option(CommonConstants.dbPassword, commonModel.password)
//        .option(CommonConstants.dbDriver, commonModel.driver).load()
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.getMetastoreAsDataFrame :: Successfully Loaded Metastore into DataFrame. ")
//      case Failure(f) => throw new Exception("CommonUtil.getMetastoreAsDataFrame :: Exception occured while Loading Metastore into DataFrame" + f.getMessage)
//    }
//    metaStoreDF
//  }
//}
