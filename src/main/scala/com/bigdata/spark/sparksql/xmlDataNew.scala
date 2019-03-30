package com.bigdata.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object xmlDataNew {
  def main(args: Array[String]) {
    //val spark = SparkSession.builder.master("local[*]").appName("xmlData").config("spark.sql.warehouse.dir", "/home/hadoop/work/warehouse").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.master("local[*]").appName("xmlDataNew").getOrCreate()
    val sc = spark.sparkContext
    val conf = new SparkConf().setAppName("xmlDataNew").setMaster("local[*]")
    //    val sc = new SparkContext(conf)
    //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val sqlContext = spark.sqlContext

    val xmlData = "C:\\work\\datasets\\xmldata\\query.xml"
    val xmlDF = spark.read.format("xml").option("rowTag","root").load(xmlData)
    xmlDF.printSchema()
    xmlDF.show()
    /*
    xmlDF.createOrReplaceTempView("xmlRaviKiran_Trans")
    val res = spark.sql("select BeginDateTime, BusinessDayDate, BU.UnitID._TypeCode BUnitTypeCode, BU.UnitID._VALUE BUnitValue, CurrencyCode, EndDateTime,OperatorID._OperatorType  OperatorIDOprType, OperatorID._VALUE OperatorIDValue, ReceiptNumber, RetailStoreID, RTLI.BeginDateTime RTLIBeginDateTime, RTLI.EndDateTime RTLIEndDateTime, RTLI.Sale.ActualSalesUnitPrice RTLISaleActualSalesUnitPrice,RTLI.Sale.Associate.AssociateID._OperatorType RTLISAIDOperatorType,RTLI.Sale.Associate.AssociateID._VALUE RTLISAIDValue, RTLI.Sale.Description RTLISDescription,RTLI.Sale.ExtendedAmount RTLISExtendedAmount, RTLI.Sale.ItemID,RTLI.Sale.ItemNotOnFileFlag RTLISaleItemNotOnFileFlag, RTLI.Sale.MerchandiseHierarchy._Level RTLISMHierarchyLevel,RTLI.Sale.MerchandiseHierarchy._VALUE RTLISMHierarchyValue, RTLI.Sale.POSIdentity.POSItemID RTLISPIdentityPOSItemID, RTLI.Sale.POSIdentity._POSIDType RTLISPIdentityPOSIDType, RTLI.Sale.Quantity._UnitOfMeasureCode RTLISQUnitOfMeasureCode, RTLI.Sale.Quantity._VALUE RTLISQuantityVALUE, RTLI.Sale.RegularSalesUnitPrice RTLISRegularSalesUnitPrice, RTLI.Sale.SellingLocation._SubDepartmentCode RTLISSLSubDepartmentCode, RTLI.Sale.SellingLocation._VALUE RTLISSLValue, RTLI.Sale.Tax.Amount RTLISaleTaxAmount,RTLI.Sale.Tax.Percent RTLISaleTaxPercent, RTLI.Sale.Tax.SequenceNumber RTLISaleTaxSequenceNumber,RTLI.Sale.Tax.TaxAuthority RTLISaleTaxTaxAuthority ,RTLI.Sale.Tax.TaxGroupID RTLISaleTaxTaxGroupID,RTLI.Sale.Tax.TaxRuleID RTLISaleTaxTaxRuleID, RTLI.Sale.Tax.TaxableAmount RTLISaleTaxTaxableAmount ,RTLI.Sale.Tax._NegativeValue RTLSaleTaxNegativeValue, RTLI.Sale.Tax._TaxType RTLISaleTaxTaxType, RTLI.Sale.Tax.`pcms:PCMSTax`.`pcms:DetailNumber`.`_TxnType` pcmstxntype, RTLI.Sale.Tax.`pcms:PCMSTax`.`pcms:DetailNumber`.`_VALUE` pcmstxnValue, RTLI.Sale.Tax.`pcms:PCMSTax`.`pcms:TaxRuleName` pcmstxRuleName, RTLI.Sale.TaxIncludedInPriceFlag RTLISaleTaxIncludedInPriceFlag,  RTLI.Sale.UnitCostPrice RTLISaleUnitCostPrice, RTLI.Sale._ItemSubType RTLISaleItemSubType, RTLI.Sale._ItemType RTLISaleItemType, RTLI.Sale._NegativeValue RTLISaleNegativeValue, RTLI.Sale.`pcms:PCMSSale`.`pcms:ExtendedAmountAllDiscounts` RTLISalepcmsPCMSSaleDiscounts, RTLI.Sale.`pcms:PCMSSale`.`pcms:LinkedItemLevel` RTLILinkedItemLevel,RTLI.Sale.`pcms:PCMSSale`.`pcms:MerchandisingFormat` RTLIMerchandisingFormat, RTLI.Sale.`pcms:PCMSSale`.`pcms:OwningDepartment` RTLIOwningDepartment, RTLI.Sale.`pcms:PCMSSale`.`pcms:PriceControl` RTLIPriceControl, RTLI.Sale.`pcms:PCMSSale`.`pcms:PriceDiscountable` RTLIPriceDiscountable, RTLI.Sale.`pcms:PCMSSale`.`pcms:ProductHandling`  RTLIProductHandling, RTLI.Sale.`pcms:PCMSSale`.`pcms:Resaleable` RTLIResaleable, RTLI.Sale.`pcms:PCMSSale`.`pcms:SelfScanPrice` RTLISelfScanPrice, RTLI.SequenceNumber RTLISequenceNumber, RTLI.Tender.Amount RTLITenderAmount,RTLI.Tender.TenderChange.Amount RTLITenderChangeAmount,RTLI.Tender.TenderChange.TenderID RTLITenderID, RTLI.Tender.TenderChange._TenderType RTLITenderTenderType,RTLI.Tender.TenderChange.`pcms:PCMSTender`.`pcms:MediaType` pcmsMediaType, RTLI.Tender.TenderID RTLITenderTenderID, RTLI.Tender._TenderType RTLITTenderType, RTLI.Tender._TypeCode RTLITTypeCode, RTLI.Tender.`pcms:PCMSTender`.`pcms:MediaType` RTLITpcmsMediaType, RTLI._EntryMethod RTLIEntryMethod, RTLI._LineType RTLILineType, RTLI.`pcms:PCMSCustomData`._Name RTLICustomDataName, RTLI.`pcms:PCMSCustomData`._VALUE RTLICustomDataValue, RTLI.`pcms:PCMSLineItem`.`pcms:DetailNumber` RTLIDetailNumber, RTLI.`pcms:PCMSRefusedSale`.Reason RTLIpcmsReason, RetailTransaction.LoyaltyAccount.CustomerID LoyaltyCustomerID, RTT._TotalType RTTTotalType, RTT._VALUE RTTValue, RetailTransaction._OutsideSalesFlag RTOutsideSalesFlag, RetailTransaction._TransactionStatus RTTransactionStatus, RetailTransaction._Version RTVersion,  RTPCLA.`pcms:CustomerType` RTPCLACustomerType, RTPCLA.`pcms:DeclinedEmail` RTPCLADeclinedEmail , RTPCLA.`pcms:EmailAddress` RTPCLAEmailAddress , RTPCLA.`pcms:LoyaltyAccountID` RTPCLALoyaltyAccountID , RTPCLA.`pcms:LoyaltyAccountStatus` RTPCLALoyaltyAccountStatus, RTPCLA.`pcms:LoyaltyCaptureMode` RTPCLALoyaltyCaptureMode , RTPCLA.`pcms:LoyaltyCardType` RTPCLALoyaltyCardType, RTPCLA.`pcms:LoyaltyProgramName` RTPCLALoyaltyProgramName , RTPCLA.`pcms:UpdatedEmail` RTPCLAUpdatedEmail, SequenceNumber, WorkstationID, `pcms:PCMSTransaction`.`pcms:BuildNumber` BuildNumber, `pcms:PCMSTransaction`.`pcms:DBSchemaVersion` DBSchemaVersion ,`pcms:PCMSTransaction`.`pcms:ExternalSystem` pcmsExternalSystem, `pcms:PCMSTransaction`.`pcms:OnlineStatus` OnlineStatus, `pcms:PCMSTransaction`.`pcms:TransactionType` TransactionType , `pcms:PCMSTransaction`.`pcms:UTCOffset` UTCOffset , `pcms:PCMSTransaction`.`pcms:WorkstationPersonality` WorkstationPersonality from xmlRaviKiran_Trans lateral view explode (BusinessUnit) T1 as BU lateral view explode (RetailTransaction.LineItem) T2 as RTLI lateral view explode (RetailTransaction.Total) T3 as RTT lateral view explode (RetailTransaction.`pcms:PCMSLoyaltyAccounts`.`pcms:LoyaltyAccount`) T4 as RTPCLA")
    res.printSchema()
    res.show()
    res.createOrReplaceTempView("NewxmlRaviKiran_Trans")
    val resNew = spark.sql("select BeginDateTime,BusinessDayDate,BUnitTypeCode,BUnitValue,CurrencyCode,EndDateTime,OperatorIDOprType," +
      "OperatorIDValue,ReceiptNumber,RetailStoreID,RTLIBeginDateTime,RTLIEndDateTime,RTLISaleActualSalesUnitPrice,RTLISAIDOperatorType," +
      "RTLISAIDValue,RTLISExtendedAmount,ItID.`_Type` ItIDType,ItID.`_VALUE` ItIDValue,RTLISMHierarchyLevel,RTLISMHierarchyValue," +
      "RTLISPIdentityPOSItemID,RTLISPIdentityPOSIDType,RTLISQUnitOfMeasureCode,RTLISQuantityVALUE,RTLISRegularSalesUnitPrice,RTLISSLSubDepartmentCode," +
      "RTLISSLValue,RTLISaleTaxAmount,RTLISaleTaxPercent,RTLISaleTaxSequenceNumber,RTLISaleTaxTaxAuthority,RTLISaleTaxTaxGroupID," +
      "RTLISaleTaxTaxRuleID,RTLISaleTaxTaxableAmount,RTLISaleTaxTaxType,pcmstxntype,pcmstxnValue,pcmstxRuleName,RTLISaleUnitCostPrice," +
      "RTLISaleItemSubType,RTLISaleItemType,RTLISalepcmsPCMSSaleDiscounts,RTLILinkedItemLevel,RTLIMerchandisingFormat,RTLIOwningDepartment," +
      "RTLIPriceControl,RTLIPriceDiscountable,RTLIProductHandling,RTLISelfScanPrice,RTLISequenceNumber,RTLITenderAmount,RTLITenderChangeAmount," +
      "RTLITenderID,RTLITenderTenderType,pcmsMediaType,RTLITenderTenderID,RTLITTenderType,RTLITTypeCode,RTLITpcmsMediaType,RTLIEntryMethod," +
      "RTLILineType,RTLICustomDataName,RTLICustomDataValue,RTBN.`_TxnType` RTBNTxnType,RTBN.`_VALUE` RTBNValue,RTLIpcmsReason,LoyaltyCustomerID," +
      "RTTTotalType,RTTValue,RTTransactionStatus,RTVersion,RTPCLACustomerType,RTPCLAEmailAddress,RTPCLALoyaltyAccountID,RTPCLALoyaltyAccountStatus," +
      "RTPCLALoyaltyCaptureMode,RTPCLALoyaltyCardType,RTPCLALoyaltyProgramName,SequenceNumber,WorkstationID,BuildNumber,DBSchemaVersion," +
      "pcmsExternalSystem,OnlineStatus,TransactionType,UTCOffset,WorkstationPersonality " +
      "from NewxmlRaviKiran_Trans lateral view explode (RTLIDetailNumber) T4 as RTBN lateral view explode (ItemID) T5 as ItID")
    resNew.printSchema()
    //resNew.show()
    /*
    resNew.createOrReplaceTempView("xmlData")
    val res1 = spark.sql("create table my_table as select * from xmlData")
    //res.show()
    res1.write.format("orc").insertInto("my_table")
    */
    resNew.write.format("orc").saveAsTable(hivexmltab)
    /*
    val url = "jdbc:oracle:thin://@oracledb.cotidg8mtnrt.ap-south-1.rds.amazonaws.com:1521/ORCL"
    val prop = new java.util.Properties()
    prop.setProperty("user","ousername")
    prop.setProperty("password","opassword")
    prop.setProperty("driver","oracle.jdbc.OracleDriver")
    resNew.write.mode("append").jdbc(url,"xmlRaviKiran_SUNILNEW",prop)
    */
    */
    spark.stop()
  }
}
