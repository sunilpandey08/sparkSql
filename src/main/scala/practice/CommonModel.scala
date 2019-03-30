//package practice
//
//
//
//import scala.collection.Map
//
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.SparkSession
//
///* Properties used across all modules in project.
//*/
//class CommonModel extends Serializable {
//
//  var sparkSession: SparkSession = _;
//  var configFile: String = _;
//  var user: String = _;
//  var password: String = _;
//  var url: String = _;
//  var driver: String = _;
//  var metastoreUdfName: String = _;
//  var udfParams: Option[List[String]] = None;
//  var table: String = _;
//  var metastoreDF: DataFrame = _;
//  var metastoreMap: Map[String, String] = _;
//  var workflowXmlFile: String = _;
//  var configPropertiesMap: Map[String, String] = _;
//  var hdfsReadPath: String = _;
//  var hdfsDeletePath: String = _;
//  var hdfsWritePath: String = _;
//  var hdfsSrcPath: String = _;
//  var hdfsTgtPath: String = _;
//  var writeMode: String = _;
//  var partitionedBy: String = _;
//  var partitions: List[String] = _;
//  var sshUserName: String = _;
//  var sshPassword: String = _;
//  var edgeNodeIp: String = _;
//  var sshPortNo: String = _;
//  var wfName: String = _;
//  var processFlag: String = _;
//  var triggerID: String = _;
//  var renameTargetFile: String = _;
//  var columnList: List[String] = _;
//  var tableName: String = _;
//  var dataBaseName: String = _;
//  var partitionColumn: Option[String] = None;
//  var oozieJobId: String = _;
//  var processStatus: String = _;
//  var srcColName: String = _;
//  var regex: String = _;
//  var replaceString: String = _;
//  var tgtColName: String = _;
//  var tgtColTrans: String = _;
//  var appId: String = _;
//  var buID: String = _;
//  var clientID: String = _;
//  var cdfName: String = _;
//  var gdfName: String = _;
//  var master_table: String = _
//  var crossWalk_table: String = _
//  var demographics_table: String = _
//  var exception_table: String = _
//  var schema: String = _
//  var goldenRecordTable: String = _
//  var impalaVersionSchema: String = _
//  var npiTaxIdList: List[String] = _
//  var sourceName: String = _
//  var exceptionPath: String = _
//  var engineType: Option[String] = None;
//  var exceptiondb: String = _
//  var exceptiontable: String = _
//  var enterpriseClientId: String = _
//  var errDesc: Option[String] = None
//  var errDescFilePath: String = _
//  var rpdTablesAsString: String = _
//  var controlTableWithDB: String = _
//  var refDataBaseName: String = _
//  var prodColDf: DataFrame = _;
//  var refColDf: DataFrame = _;
//  var genericFieldValDF: DataFrame = _;
//  var empGrpDf: DataFrame = _;
//  var joinConditionsDf: DataFrame = _;
//  var partitionDf: DataFrame = _;
//  var colMaxOrdinalVal: Int = _;
//  var provMdmDatabase: String = _;
//  var provCrossWalkTable: String = _;
//  var patienIdDfNullFlag: String = _;
//  var prodSparkTempTableColumns: String = _;
//  var impalaErSqlAbsoluteFilePath: String = _;
//  var impalaErSqlFile: String = _;
//  var applicationId: String = _;
//
//  var listFiles: String = _;
//  var localDxcgGapmodelPath: String = _;
//  var patientIdColFlag: Boolean = _;
//  var provErflag: Boolean = _;
//}