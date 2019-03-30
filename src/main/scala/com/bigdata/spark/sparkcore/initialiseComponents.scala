//package com.bigdata.spark.sparkcore
//
//
//import scala.util.Failure
//import scala.util.Success
//import scala.util.Try
//import org.apache.spark.sql.SparkSession
//import com.verscend.common.constants.CommonConstants
//import com.verscend.common.constants.DelimiterConstants
//import com.verscend.common.constants.SshExecConnectivityConstants
//import com.verscend.common.logger.LogAgent
//import com.bigdata.spark.sparkcore.sparkKafkaConsumer
//import com.bigdata.spark.sparkstreaming.sparkKafkaConsumer
//import com.verscend.common.util.CommonUtil
//import com.verscend.common.util.EncryptDecryptAlg
//
///* Initialising Spark Session and assigning it to Model.
//* Reading property File and assigning them to Model.
//* Method call to Read envconfig from Oralce Metastore and assigning them to Model.
//* @param - IN - String
//* @param - Out - CommonModel
//*/
//class Initialise extends Serializable {
//
//  def initialiseComponents(configFile: String): CommonModel = {
//
//    // Creating an object for CommonModel
//    val commonModel = new CommonModel;
//
//
//    Try {
//
//      // Initialising sparksession and setting to CommonModel
//      commonModel.sparkSession = SparkSession.builder().appName(CommonConstants.appName).getOrCreate();
//      // Reading config file and converting it to key,value pair.
//      commonModel.configFile = configFile
//      commonModel.test = commonModel.configPropertiesMap.get(CommonConstants.user).mkString;
//      commonModel.configPropertiesMap = getConfigPropertiesMap(commonModel)
//
//      // Getting parameters needed to connect to Oracle Metastore from the config property map and set it to CommonModel
//      commonModel.user = commonModel.configPropertiesMap.get(CommonConstants.user).mkString;
//      commonModel.password = EncryptDecryptAlg.decrypt(commonModel.configPropertiesMap.get(CommonConstants.password).mkString, commonModel.configPropertiesMap.get(CommonConstants.privateKeyPath).mkString);
//      commonModel.url = commonModel.configPropertiesMap.get(CommonConstants.url).mkString;
//      commonModel.driver = commonModel.configPropertiesMap.get(CommonConstants.driver).mkString;
//
//      // Getting parameters needed to connect to Edge Node for Remote commands from the config property map and set it to CommonModel
//      commonModel.sshUserName = commonModel.configPropertiesMap.get(SshExecConnectivityConstants.sshUserName).mkString;
//      commonModel.sshPassword = EncryptDecryptAlg.decrypt(commonModel.configPropertiesMap.get(SshExecConnectivityConstants.sshPassword).mkString, commonModel.configPropertiesMap.get(CommonConstants.privateKeyPath).mkString);
//      commonModel.edgeNodeIp = commonModel.configPropertiesMap.get(SshExecConnectivityConstants.edgeNodeIp).mkString;
//      commonModel.sshPortNo = commonModel.configPropertiesMap.get(SshExecConnectivityConstants.sshPortNo).mkString;
//
//      // Reading env config from Metastore, converting it to a Map with key,value pair and setting it to CommonModel
//      commonModel.metastoreUdfName = CommonConstants.udfGetEnvConfDtls
//      commonModel.table = CommonUtil.getMetastoreFunction(commonModel);
//      commonModel.metastoreDF = CommonUtil.getMetastoreAsDataFrame(commonModel);
//      commonModel.metastoreMap = CommonUtil.getMetastoreAsMap(commonModel);
//
//    } match {
//      case Success(s) => LogAgent.log.info("Initialise.initialiseComponents :: Successfully initialised the components ")
//      case Failure(f) =>
//        if (commonModel.errDesc == None)
//          commonModel.errDesc = Some("Initialise.initialiseComponents :: Exception occured while initialising the components"+f.getMessage)
//        throw new Exception("Initialise.initialiseComponents :: Exception occured while initialising the components " + f.getMessage)
//    }
//    commonModel;
//  }
//
//  /* Convert a config.properties file data into Map with Key,Value pair.
//* @param - IN - CommonModel
//* @param - Out - Map[String,String]
//*/
//
//  def getConfigPropertiesMap(commonModel: CommonModel): scala.collection.Map[String, String] = {
//
//    var configPropertiesMap = scala.collection.Map[String, String]();
//    Try {
//      val configPropertiesDF = commonModel.sparkSession.read.text(commonModel.configFile)
//      configPropertiesMap = configPropertiesDF.rdd.map(record => {
//        val recordArray = record.get(0).toString().split(DelimiterConstants.COLON + DelimiterConstants.EQUAL);
//        if (recordArray.length > 1) (recordArray(0), recordArray(1))
//        else (recordArray(0), "")
//      })
//        .map { case (x, y) => (x.asInstanceOf[String], y.asInstanceOf[String]) }.collectAsMap();
//
//    } match {
//      case Success(s) => LogAgent.log.info("Initialise.getConfigPropertiesMap :: Successfully converted config.properties file's data into Map with Key,Value pair. ")
//      case Failure(f) =>
//        if (commonModel.errDesc == None)
//          commonModel.errDesc = Some("Initialise.getConfigPropertiesMap :: Exception occured while Converting config.properties file's data into Map with Key,Value pair" +f.getMessage)
//        throw new Exception("Initialise.getConfigPropertiesMap :: Exception occured while Converting config.properties file's data into Map with Key,Value pair" + f.getMessage)
//    }
//    configPropertiesMap;
//  }
//
//}
//
