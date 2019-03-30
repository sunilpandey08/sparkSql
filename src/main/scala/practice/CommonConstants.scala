//package practice
//
//
///* Contains keys and their values which are used across all modules.
//*/
//object CommonConstants extends Enumeration {
//
//  val appName = "Verscend"
//  val url = "url"
//  val driver = "driver"
//  val user = "user"
//  val password = "password"
//  val dbUrl = "url"
//  val dbDriver = "driver"
//  val dbUser = "user"
//  val dbPassword = "password"
//  val jdbcFormat = "jdbc"
//  val dbTable = "dbtable"
//  val udfGetEnvConfDtls = "UDF_GET_ENV_CONFIG_DETAILS"
//  val udfGenerateWFXml = "GET_UDF_GENERATE_WF_XML"
//  val hdfsURI = "HDFS_DIR_IP"
//  val workflowXmlPath = "WORKFLOW_XML_PATH"
//  val dateFormat = "DATE_FORMAT"
//  val xmlExtnsn = "WORKFLOW_XML_EXTNSN"
//  val workflowXmlBaseName = "WORKFLOW_XML_FILENAME"
//  val hdfsDefaultFs = "fs.defaultFS"
//  val oozieClientUrl = "OOZIE_CLIENT_URL"
//  val nameNode = "NAME_NODE"
//  val jobTracker = "JOB_TRACKER"
//  val queue = "QUEUE"
//  val oozieLibPath = "OOZIE_LIBPATH"
//  val oozieUseSysLibPath = "OOZIE_USE_SYSTEM_LIBPATH"
//  val oozieWFRerunFailNodes = "OOZIE_WF_RERUN_FAILNODES"
//  val oozieJobTracker = "jobTracker"
//  val oozieNameNode = "nameNode"
//  val oozieQueueName = "queueName"
//  val oozieLibraryPath = "oozie.libpath"
//  val oozieUseLibSysPath = "oozie.use.system.libpath"
//  val oozieWFRerunFailNode = "oozie.wf.rerun.failnodes"
//  val strictHostKeyChecking = "StrictHostKeyChecking"
//  val execute = "exec"
//  val shellExecute = "sh"
//  val sqoop = "sqoop"
//  val permission = "777"
//  val appendPartition = "0"
//  val refreshPartition = "1"
//  val publicKeyPath = "PUBLIC_KEY_PATH"
//  val privateKeyPath = "PRIVATE_KEY_PATH"
//  val encryptionAlgorithm = "RSA"
//  val encrypt = "encrypt"
//  val decrypt = "decrypt"
//  val generateKey = "generatekey"
//  val date = "date"
//  val stringFormat = "string"
//  val wfInstanceUpsertFunctionName = "UDF_EXEC_WF_INSTANCE"
//  val wfTriggerUpsertFunctionName = "UDF_EXEC_WF_TRIGGER"
//  val failureFlag = "F"
//  val zeroVal = "0"
//  val failureStatusCode = "failed"
//  val processedSuccessFlag = "Y"
//  val succesStatusCode = "succeeded"
//  val inProcessFlag = "P"
//  val inProccessStatusCode = "processing"
//  val mdmWorkflowName = "MDM"
//  val processedToFlag = "N"
//  val prodMode = "append"
//  val clientName = "client_name"
//  val defaultWorkflowName = "DEFAULT"
//  val refMdmFunctionName = "UDF_TGT_MDM_MAPPING"
//  val genericFieldFunction = "UDF_GET_GENERIC_FIELD_VALUES"
//  val clientId = "client_id"
//  val buId = "bu_id"
//  val enterpriseClientId = "enterprise_client_id"
//  val applicationId = "application_id"
//  val defaultValue = "0"
//  val erLinkingWfName = "DEFAULT_ER_LINKING"
//  val processHoldFlag = "H"
//  val rawRefFunctionName = "UDF_GET_ATTRIBUTE_MAPPING"
//  val refProdFunctionName = "UDF_MAPPING_PROD"
//  val partitionFunctionName = "UDF_GET_DATA_TRANSFORMATIONS"
//  val rptLvlGenericFunctionName = "UDF_RPTLVL_GENERICFIELD_VALUES"
//  val wfTriggerUpsertRawFuncName = "UDF_EXEC_WF_TRIGGER_RAW"
//  val wfMdmErDetailsFunctionName = "UDF_MDM_ER_DETAILS"
//  val refMode = "overwrite"
//  val re_partn_read_val = "RE_PARTN_READ_VAL"
//  val re_partn_write_val = "RE_PARTN_WRITE_VAL"
//  val overwrite = "overwrite"
//  val dataTgtTable = "data_tgt"
//  val udf_mdm_lookup_attr = "UDF_GET_LOOKUP_ATTRIBUTE"
//  val coalescePartitions = 1000
//  val nullPartition = "null_partition"
//  val providerSk = "PROVIDER_SK"
//  val rdm = "rdm"
//  val sourceSystem = "SOURCE_SYSTEM"
//  val provider = "provider"
//  val bu_name = "bu_name"
//  val partScheme = "PARTITIONING_SCHEME"
//  val appid = "application_id"
//  val versioningImpala = "versioningimpala"
//  val impalaServer = "IMPALA_SERVER"
//  val impalaVersioningDb = "IMPALA_VERSION_DB"
//  val dataDistributionUdf = "udf_dist_get_attribute_mapping"
//  val error_log_path = "ERROR_LOG_PATH"
//  val erLinkingConfigFunctionName = "udf_get_mdm_erlink_info"
//  val regExpVal = "[^a-zA-Z0-9\u00E0-\u00FC]+"
//  val impalaErQueryPath = "ER-LINKING_IMPALA_PATH"
//  val sqlExtension = "SQL_EXTENSION"
//  val tempStage = "PRODUCTION_TEMP_STAGE"
//  val homePath = "HOME_PATH"
//  val errorDesc = "Count Mismatch"
//  val incompleteStatusCode = "I"
//  val secSrcMDMWorkflowName = "SEC_SRC_ST0_MASTERING"
//  val rpdClientName = "RPD_CLIENT_NAME"
//  val environment = "ENV"
//
//}
