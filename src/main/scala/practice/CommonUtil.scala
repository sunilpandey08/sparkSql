//package practice
//
//
//import scala.util.Failure
//import scala.util.Success
//import scala.util.Try
//
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.functions.date_format
//import org.apache.spark.sql.functions.lit
//import org.apache.spark.sql.functions.regexp_replace
//import org.apache.spark.sql.functions.when
//
//import com.verscend.common.constants.CommonConstants
//import com.verscend.common.logger.LogAgent
//import com.verscend.common.model.CommonModel
//
//object CommonUtil extends Serializable {
//
//  /* Reading Oracle Metastore and loading it into a DataFrame.
//* @param - IN - CommonModel
//* @param - Out - DataFrame
//*/
//
//  def getMetastoreAsDataFrame(commonModel: CommonModel): DataFrame = {
//    var metaStoreDF = commonModel.sparkSession.emptyDataFrame;
//    Try {
//      metaStoreDF = aaa.sparkSession.read.format(CommonConstants.jdbcFormat)
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
//
//  /* Convert a dataFrame with Key,Value to Map with Key,Value pair.
//* @param - IN - CommonModel
//* @param - Out - Map[String,String]
//*/
//
//  def getMetastoreAsMap(commonModel: CommonModel): scala.collection.Map[String, String] = {
//
//    var metaStoreMap = scala.collection.Map[String, String]();
//    Try {
//      metaStoreMap = commonModel.metastoreDF.rdd.map(record => (record.get(0), record.get(1)))
//        .map { case (x, y) => (x.asInstanceOf[String], y.asInstanceOf[String]) }.collectAsMap();
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.getMetastoreAsMap :: Successfully Converted Metastore DataFrame to Map. ")
//      case Failure(f) => throw new Exception("CommonUtil.getMetastoreAsMap :: Exception occured while Converting Metastore DataFrame to Map" + f.getMessage)
//    }
//    metaStoreMap;
//  }
//
//  /* Append Slash("/") at the end of given file path if it is not there.
//     * @param - IN - String
//     * @param - Out - String
//     */
//  def appendSlashAtEndOfFilePathIfNotThere(filePath: String): String = {
//    var filePathWithAppend = "";
//    Try {
//      if (filePath.endsWith("/"))
//        filePathWithAppend = filePath
//      else
//        filePathWithAppend = filePath + "/"
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.appendSlashAtEndOfFilePathIfNotThere :: Successfully Added Slash to FilePath. ")
//      case Failure(f) => throw new Exception("CommonUtil.appendSlashAtEndOfFilePathIfNotThere :: Exception occured while adding slash" + f.getMessage)
//    }
//    filePathWithAppend
//  }
//
//  /* Build dynamic Query to connect to Oracle Metastore.
//* @param - IN - CommonModel
//* @param - Out - String
//*/
//
//  def getMetastoreFunction(commonModel: CommonModel): String = {
//    var metastoreFunc = "";
//    Try {
//      metastoreFunc = "(select * from table(" + commonModel.metastoreUdfName + "(";
//      commonModel.udfParams match {
//        case Some(name) => {
//          commonModel.udfParams.get.foreach(arg => metastoreFunc = metastoreFunc + "'" + arg + "',")
//          metastoreFunc = metastoreFunc.dropRight(1)
//        }
//        case None       => println("Null Value found")
//      }
//      metastoreFunc = metastoreFunc + ")))";
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.getMetastoreFunction :: Successfully generated metastoreFunction. ")
//      case Failure(f) => throw new Exception("CommonUtil.getMetastoreFunction :: Exception occured while generating metastoreFunction" + f.getMessage)
//    }
//    metastoreFunc;
//  }
//
//  /* Replace the date fields as per the input format.
//* @param - IN - CommonModel,
//* @param - IN - DataFrame
//* @param - Out - DataFrame
//*/
//  def validateDateAndReplace(df: DataFrame, commonModel: CommonModel): DataFrame =
//  {
//    var newDfAfterValidation = commonModel.sparkSession.emptyDataFrame;
//    Try {
//      newDfAfterValidation = df.withColumn(commonModel.srcColName, regexp_replace(df(commonModel.srcColName), commonModel.regex, commonModel.replaceString))
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.validateDateAndReplace :: Date Validated successfully")
//      case Failure(f) => throw new Exception("CommonUtil.validateDateAndReplace :: Date not Validated. Error messages are:" + f.getMessage)
//    }
//    newDfAfterValidation
//  }
//
//  /* Convert the date format to another format as per the input format.
//* @param - IN - CommonModel
//* @param - IN - DataFrame
//* @param - Out - DataFrame
//*/
//  def convertDateToFormat(Df: DataFrame, commonModel: CommonModel): DataFrame =
//  {
//    var newDfWithStandardDate = commonModel.sparkSession.emptyDataFrame;
//    Try {
//      newDfWithStandardDate = Df.withColumn(commonModel.tgtColName, date_format(col(commonModel.srcColName).cast(CommonConstants.date), commonModel.tgtColTrans))
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.convertDateToFormat :: Date Formatted successfully")
//      case Failure(f) => throw new Exception("CommonUtil.convertDateToFormat ::  Date not formatted. Error messages are:" + f.getMessage)
//    }
//    newDfWithStandardDate
//  }
//
//  /** Modify this function to accept only common model  after MDM refactor */
//  /**
//    * Creating Partition Based on Date
//    */
//  def createPartitioneBasedOnDateColumn(Df: DataFrame, srcColName: String, tgtColName: String, tgtColTrans: String, commonModel: CommonModel): DataFrame = {
//    var partitionedDf = commonModel.sparkSession.emptyDataFrame
//    Try {
//      partitionedDf = Df.withColumn(tgtColName, when(col(srcColName).isNull, lit(CommonConstants.nullPartition)).otherwise(date_format(col(srcColName).cast(CommonConstants.date), tgtColTrans).cast(CommonConstants.stringFormat)))
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.createPartitioneBasedOnDateColumn :: Successfully created partitione.")
//      case Failure(f) => throw new Exception("CommonUtil.createPartitioneBasedOnDateColumn :: Exception occured while partitioning DataFrame: " + f.getMessage)
//    }
//    partitionedDf
//  }
//
//  /** Modify this function to accept only common model  after MDM refactor */
//  /**
//    * Creating Partition Based on Source Entity
//    */
//  def createPartitionBasedOnSrcColumn(Df: DataFrame, srcColName: String, tgtColName: String, CdfName: String, commonModel: CommonModel): DataFrame = {
//    var partitionedDf = commonModel.sparkSession.emptyDataFrame
//    Try {
//      partitionedDf = Df.withColumn(tgtColName, lit(CdfName))
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.createPartitionBasedOnSrcColumn :: Successfully created partitione.")
//      case Failure(f) => throw new Exception("CommonUtil.createPartitionBasedOnSrcColumn :: Exception occured while partitioning DataFrame: " + f.getMessage)
//    }
//    partitionedDf
//  }
//
//  /* Convert a dataframe to a list by fetching specific columns.
//* @param - IN - String
//* @param - IN - DataFrame
//* @param - Out - List[String]
//*/
//  def convertDataFrameColumnsToList(df: DataFrame, columnName: String): List[String] = {
//    var partList: List[String] = List()
//    Try {
//      partList = df.select(columnName).rdd.map(r => r(0).toString()).collect().toList
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.convertDataFrameColumnsToList :: Successfully done conversion of DataFrame columns intio List")
//      case Failure(f) => throw new Exception("CommonUtil.convertDataFrameColumnsToList :: Exception occured while conversion of DataFrame columns intio List " + f.getMessage)
//    }
//    partList
//  }
//
//  /* Replace the ssn value as per the input format.
//* @param - IN - CommonModel
//* @param - IN - DataFrame
//* @param - Out - DataFrame
//*/
//
//  def ssnValidation(df: DataFrame, commonModel: CommonModel): DataFrame =
//  {
//    var newDfWithSSN = commonModel.sparkSession.emptyDataFrame;
//    Try {
//      newDfWithSSN = df.withColumn(commonModel.srcColName, regexp_replace(df(commonModel.srcColName), commonModel.tgtColTrans, "NULL"))
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.ssnValidation :: Successfully validated SSN and Converted to proper format")
//      case Failure(f) => throw new Exception("CommonUtil.ssnValidation :: Exception occured while validating and converting ssn to proper format: " + f.getMessage)
//    }
//    newDfWithSSN
//  }
//
//  /* Insert or Update workflow trigger table
//* @param - IN - CommonModel
//* @param - Out - Unit
//*/
//  def upsertWorkflowTrigger(commonModel: CommonModel): Unit = {
//
//    Try {
//
//      commonModel.udfParams = Some(List(commonModel.appId, commonModel.buID, commonModel.clientID, commonModel.cdfName, commonModel.processFlag, commonModel.wfName, commonModel.triggerID))
//      commonModel.table = getMetastoreFunction(commonModel)
//      val upsertWfTrigger = getMetastoreAsDataFrame(commonModel).count()
//
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.upsertWfTriggerMdmMaster:: WF Trigger updated succesfully")
//      case Failure(f) => throw new Exception("CommonUtil.upsertWfTriggerMdmMaster :: WF Trigger update failed" + f.getMessage())
//    }
//  }
//
//  /* Insert or Update workflow instance table
//* @param - IN - CommonModel
//* @param - Out - Unit
//*/
//  def upsertWorkflowInstance(commonModel: CommonModel): Unit = {
//
//    Try {
//      commonModel.udfParams = Some(List(commonModel.triggerID, commonModel.oozieJobId, commonModel.processStatus))
//      commonModel.metastoreUdfName = CommonConstants.wfInstanceUpsertFunctionName
//      commonModel.table = getMetastoreFunction(commonModel)
//
//      val upsertWfTrigger = getMetastoreAsDataFrame(commonModel).count()
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.upsertWfInstance:: WF Instance updated succesfully")
//      case Failure(f) => throw new Exception("CommonUtil.upsertWfInstance :: WF Instance update failed" + f.getMessage())
//    }
//  }
//
//  /* Insert or Update workflow trigger table with failure records
//* @param - IN - CommonModel
//* @param - Out - Unit
//*/
//  def upsertWorkflowTriggerFailure(commonModel: CommonModel): Unit = {
//    Try {
//      commonModel.metastoreUdfName = CommonConstants.wfTriggerUpsertFunctionName
//      commonModel.processFlag = CommonConstants.failureFlag
//      commonModel.appId = CommonConstants.zeroVal
//      commonModel.buID = CommonConstants.zeroVal
//      commonModel.clientID = CommonConstants.zeroVal
//      upsertWorkflowTrigger(commonModel)
//
//      commonModel.processStatus = CommonConstants.failureStatusCode
//      upsertWorkflowInstance(commonModel)
//    } match {
//      case Success(s) => LogAgent.log.info("CommonUtil.upsertWorkflowTriggerFailure:: WF Trigger for failure status updated succesfully")
//      case Failure(f) => throw new Exception("CommonUtil.upsertWorkflowTriggerFailure :: WF Trigger update for failure status failed" + f.getMessage())
//    }
//  }
//
//}
