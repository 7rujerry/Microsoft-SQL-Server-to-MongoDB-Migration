# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 04:10:26 2019

@author: thetr
"""
import pyodbc
import pandas 
import codecs

server = 'localhost'
database = 'AdventureWorks2016'
username = 'ism4211'
password = 'finalproject'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+password)
cursor = cnxn.cursor()

############################################################################ Classes ############################################################################

class Cleaning:
    def __init__(self, query):
        self.query = query
   
    def filterQuery(self):
        sqlJson = (self.query).fetchall()
        fixBatch = ''.join(str(elem[0]) for elem in sqlJson)
        stripBracket=fixBatch.strip('[]')
        
        return(stripBracket)

class makeDataFrame:
    def __init__(self, query):
        self.query=query
        
    def conversion(self): 
        jsonList = []
        rowNum = (self.query).count("}")
        self.query = self.query.split(',{')
        
        for pos in range(rowNum):
            lineNew=(self.query)[pos].strip()
            lineNew=lineNew.strip('[')
            lineNew=lineNew.strip("'")
            
            if lineNew[0] == "{":
                lineList = lineNew.split(',')
                jsonList.append(lineList)
            
            else:
                lineNew = "{" +lineNew
                line_to_list = lineNew.split(',')
                jsonList.append(line_to_list)
              
                
        df =  pandas.DataFrame(jsonList)
        return(df)
        
class makeFile:
    def __init__(self, dataframe, filename):
        self.dataframe = dataframe
        self.filename = filename
        
    def saveFile(self):
       backToList = (self.dataframe).values.tolist()
       flatList = [size for sublist in backToList for size in sublist]
       string = ', '.join(flatList)
       insertMany = 'db.'+(self.filename)+'.insertMany(['+string+'])' 
       file = codecs.open((self.filename)+".js","w", "utf-8")
       file.write(insertMany) 
       file.close()
       return(file)

############################################################################ Batch 1 ############################################################################
        
hrQuery = cursor.execute(
"""
SELECT x.*
FROM HumanResources.Employee y
JOIN(
SELECT HRE.BusinessEntityID, 
HRE.NationalIDNumber, HRE.JobTitle,
HRE.LoginID, HRE.BirthDate, HRE.MaritalStatus, HRE.HireDate,
HRE.Gender, HRE.VacationHours, HRE.SickLeaveHours, HRE.SalariedFlag,
HREPH.PayFrequency, MAX(HREPH.Rate) AS 'Rate', HRD.[Name] AS 'Department',
HRD.GroupName, HRS.[Name] AS 'Shift', HRS.StartTime, HRS.EndTime
FROM HumanResources.Employee AS HRE,
HumanResources.EmployeeDepartmentHistory AS HREDH,
HumanResources.Department AS HRD,
HumanResources.[Shift] AS HRS,
HumanResources.EmployeePayHistory AS HREPH
WHERE HRE.BusinessEntityID=HREPH.BusinessEntityID
AND HRE.BusinessEntityID=HREDH.BusinessEntityID
AND HREDH.ShiftID=HRS.ShiftID
AND HREDH.DepartmentID=HRD.DepartmentID
AND HREDH.EndDate IS NULL
GROUP BY HRE.BusinessEntityID, HRE.LoginID, HRE.NationalIDNumber, HRE.JobTitle,
HRE.BirthDate, HRE.MaritalStatus,
HRE.HireDate, HRE.Gender, HRE.SalariedFlag,
HRE.VacationHours, HRE.SickLeaveHours, 
HREPH.PayFrequency, HRD.[Name], HRD.GroupName,
HRS.[Name], HRS.StartTime, HRS.EndTime) AS x
ON y.BusinessEntityID=x.BusinessEntityID
FOR JSON AUTO;
""")
       
hrJSON = Cleaning(hrQuery).filterQuery() 

hrDataframe = makeDataFrame(hrJSON).conversion()         

hrDataframe[3] = [x.strip().replace('\\\\', '\\') for x in hrDataframe[3]]
hrDataframe[8] = ('"ExcusedHours":{ '+ hrDataframe[8])
hrDataframe[9] = (hrDataframe[9]+' }')
hrDataframe[10] = ('"PayInfo":{ ' + hrDataframe[10])
hrDataframe[12] = (hrDataframe[12]+' }')
hrDataframe[15] = ('"WorkInfo":{ '+ hrDataframe[15])
hrDataframe[17] = (hrDataframe[17]+' }')

makeFile(hrDataframe, "HumanResourceCollection").saveFile()

############################################################################ Batch 2 ############################################################################

personQuery = cursor.execute(
"""
SELECT x.*
FROM Person.Person y
JOIN(
SELECT PP.BusinessEntityID, 
COALESCE(PP.Title,'NA') AS 'Title', 
PP.FirstName, 
COALESCE(PP.MiddleName,'NA') AS 'MiddleName', 
PP.LastName, 
COALESCE(PCT.[Name],'NA') AS 'Job Title', 
PEA.EmailAddress, 
PPASS.PasswordHash, 
PPASS.PasswordSalt, 
COALESCE(PAT.[Name],'NA') AS 'AddressType', 
COALESCE(REPLACE(PA.AddressLine1,',','!!!'),'NA') AS 'AddressLine1',
COALESCE(PA.AddressLine2,'NA') AS 'AddressLine2', 
COALESCE(PA.City,'NA') AS 'City', 
COALESCE(PA.PostalCode,'NA') AS 'PostalCode', 
COALESCE(PSP.[Name],'NA') AS 'Place', 
COALESCE(PCR.[Name],'NA') AS 'Country',
COALESCE(LEFT(REPLACE(REPLACE(TRY_CONVERT(nvarchar(100),PA.SpatialLocation),'POINT (','['),')',']'), CHARINDEX(' ',REPLACE(REPLACE(TRY_CONVERT(nvarchar(100),PA.SpatialLocation),'POINT (','['),')',']'))-1),'NA') AS 'Longitude',
COALESCE(SUBSTRING(REPLACE(REPLACE(REPLACE(TRY_CONVERT(nvarchar(100),PA.SpatialLocation),'POINT (',''),')',''),' ','!'), CHARINDEX('!', REPLACE(REPLACE(REPLACE(TRY_CONVERT(nvarchar(100),PA.SpatialLocation),'POINT (',''),')',''),' ','!'), 0), 100), 'NA') AS 'Latitude',
PPP.PhoneNumber,
PPNT.[Name] AS 'PhoneType'
FROM Person.Person PP
LEFT JOIN Person.BusinessEntityContact PBEC
ON PBEC.PersonID=PP.BusinessEntityID
LEFT JOIN Person.ContactType PCT
ON PBEC.ContactTypeID=PCT.ContactTypeID
JOIN Person.EmailAddress PEA
ON PP.BusinessEntityID=PEA.BusinessEntityID
JOIN Person.PersonPhone PPP
ON PP.BusinessEntityID=PPP.BusinessEntityID
JOIN Person.PhoneNumberType PPNT
ON PPP.PhoneNumberTypeID=PPNT.PhoneNumberTypeID
JOIN Person.[Password] PPASS
ON PP.BusinessEntityID=PPASS.BusinessEntityID
LEFT JOIN Person.BusinessEntity PBE
ON PP.BusinessEntityID=PBE.BusinessEntityID
LEFT JOIN Person.BusinessEntityAddress AS PBEA
ON PBE.BusinessEntityID=PBEA.BusinessEntityID
LEFT JOIN Person.[AddressType] AS PAT
ON PBEA.AddressTypeID=PAT.AddressTypeID
LEFT JOIN Person.[Address] AS PA
ON PBEA.AddressID=PA.AddressID
LEFT JOIN Person.StateProvince AS PSP
ON PA.StateProvinceID=PSP.StateProvinceID
LEFT JOIN Person.CountryRegion AS PCR
ON PSP.CountryRegionCode=PCR.CountryRegionCode) AS x
ON y.BusinessEntityID=x.BusinessEntityID
FOR JSON AUTO;
""")

personJSON = Cleaning(personQuery).filterQuery()
  
personDataframe = makeDataFrame(personJSON).conversion()

duplicateRowsPerson = personDataframe[personDataframe.duplicated([0])]

updatePerson = duplicateRowsPerson[[0,9,10,11,12,13,14,15,16,17]] 

personDataframe = personDataframe[~personDataframe.index.isin(duplicateRowsPerson.index)] 

personDataframe[6] = ('"Credentials":{'+ personDataframe[6])
personDataframe[8] = (personDataframe[8]+'}')
personDataframe[9]=('"Location": [{'+ personDataframe[9])
personDataframe[16]=('"Geospatial Location":{ "type":"Point",' + personDataframe[16])
personDataframe[17]=(personDataframe[17]+']}')
personDataframe[18]=('"PhoneInfo":{'+ personDataframe[18])
personDataframe[19]=(personDataframe[19]+'}')

makeFile(personDataframe, "personBusinessEntityCollection").saveFile()

fin = open("personBusinessEntityCollection.js","rt")
data = fin.read()
data = data.replace('"Title":"NA",','')
data = data.replace('"MiddleName":"NA",','')
data = data.replace('"Job Title":"NA",','')
data = data.replace('"Location": [{"AddressType":"NA", "AddressLine1":"NA", "AddressLine2":"NA", "City":"NA", "PostalCode":"NA", "Place":"NA", "Country":"NA", "Geospatial Location":{ "type":"Point","Longitude":"NA", "Latitude":"NA"]},','')
data = data.replace('"AddressLine2":"NA",','')
data = data.replace('"Longitude":"','"coordinates":')
data = data.replace('", "Latitude":"!',',')
data = data.replace('"]}, ',']}}],')
data = data.replace('\/','/')
data = data.replace('!!!',',')
data = data.replace('personBusinessEntityCollection','BusinessEntityCollection')
fin = open("personBusinessEntityCollection.js","wt")
fin.write(data)
fin.close()

updatePerson[9]=('"Location": [{'+ updatePerson[9])
updatePerson[16]=('"Geospatial Location":{ "type":"Point",' + updatePerson[16])
updatePerson[17]=(updatePerson[17]+']}')

makeFile(updatePerson, "updateBusinessEntityCollection").saveFile()

fin = open("updateBusinessEntityCollection.js","rt")
data = fin.read()
data = data.replace('db.updateBusinessEntityCollection.insertMany([','')
data = data.replace('{"BusinessEntityID":','db.BusinessEntityCollection.update({"BusinessEntityID":')
data = data.replace('"AddressLine2":"NA",','')
data = data.replace(', "Location": [','}, {$push :{"Location":')
data = data.replace('"Longitude":"','"coordinates":')
data = data.replace('", "Latitude":"!',',')
data = data.replace('"]},',''']}}}})

''')
data = data.replace('"]}])',']}}}})')
fin = open("updateBusinessEntityCollection.js","wt")
fin.write(data)
fin.close()

vendorQuery = cursor.execute(
'''
SELECT x.*
FROM Purchasing.Vendor y
JOIN(
SELECT PV.BusinessEntityID, 
PV.AccountNumber, 
REPLACE(PV.Name, ',', '!') AS 'Vendor Name', 
PV.CreditRating, 
PV.PreferredVendorStatus,
PV.ActiveFlag, 
COALESCE(PV.PurchasingWebServiceURL, 'NA') AS 'PurchasingWebServiceURL', 
PA.AddressLine1,
COALESCE(PA.AddressLine2, 'NA') AS 'AddressLine2', 
PA.City, 
PA.PostalCode, 
PSP.[Name] AS 'Place', 
PCR.[Name] AS 'Country',
COALESCE(LEFT(REPLACE(REPLACE(TRY_CONVERT(nvarchar(100),PA.SpatialLocation),'POINT (','['),')',']'), CHARINDEX(' ',REPLACE(REPLACE(TRY_CONVERT(nvarchar(100),PA.SpatialLocation),'POINT (','['),')',']'))-1),'NA') AS 'Longitude',
COALESCE(SUBSTRING(REPLACE(REPLACE(REPLACE(TRY_CONVERT(nvarchar(100),PA.SpatialLocation),'POINT (',''),')',''),' ','!'), CHARINDEX('!', REPLACE(REPLACE(REPLACE(TRY_CONVERT(nvarchar(100),PA.SpatialLocation),'POINT (',''),')',''),' ','!'), 0), 100), 'NA') AS 'Latitude'
FROM Purchasing.Vendor PV 
JOIN Person.BusinessEntity PBE
ON PV.BusinessEntityID=PBE.BusinessEntityID
LEFT JOIN Person.BusinessEntityAddress AS PBEA
ON PBE.BusinessEntityID=PBEA.BusinessEntityID
LEFT JOIN Person.[AddressType] AS PAT
ON PBEA.AddressTypeID=PAT.AddressTypeID
LEFT JOIN Person.[Address] AS PA
ON PBEA.AddressID=PA.AddressID
LEFT JOIN Person.StateProvince AS PSP
ON PA.StateProvinceID=PSP.StateProvinceID
LEFT JOIN Person.CountryRegion AS PCR
ON PSP.CountryRegionCode=PCR.CountryRegionCode) AS x
ON y.BusinessEntityID=x.BusinessEntityID
FOR JSON AUTO;
'''
)

vendorJSON = Cleaning(vendorQuery).filterQuery()
  
vendorDataframe = makeDataFrame(vendorJSON).conversion()

vendorDataframe[7]=('"Location": [{'+ vendorDataframe[7])
vendorDataframe[13]=('"Geospatial Location":{ "type":"Point",' + vendorDataframe[13])
vendorDataframe[14]=(vendorDataframe[14]+'}}')

makeFile(vendorDataframe, "vendorBusinessEntityCollection").saveFile()

fin = open("vendorBusinessEntityCollection.js","rt")
data = fin.read()
data = data.replace('\/','/')
data = data.replace('"AddressLine2":"NA",','')
data = data.replace('"PurchasingWebServiceURL":"NA",','')
data = data.replace('", "Latitude":"!',',')
data = data.replace('"Longitude":"','"coordinates":')
data = data.replace('"}}}',']}}]}')
data = data.replace('vendorBusinessEntityCollection','BusinessEntityCollection')
fin = open("vendorBusinessEntityCollection.js","wt")
fin.write(data)
fin.close()

############################################################################ Batch 3 ############################################################################

purchaseQuery = cursor.execute(
'''
SELECT x.*
FROM Purchasing.PurchaseOrderDetail y
JOIN(
SELECT PPOD.PurchaseOrderID,
PPOH.VendorID, 
PPOH.EmployeeID,  
PPOD.PurchaseOrderDetailID, 
REPLACE(PP.Name,',','!') AS 'ProductName',
PSM.Name AS 'ShippingMethod', 
CONVERT(VARCHAR(20), PPOH.OrderDate, 101) AS 'OrderDate',
CONVERT(VARCHAR(20), PPOH.ShipDate, 101) AS 'ShipDate',
PPOD.OrderQty,
PPOD.UnitPrice,
PPOD.ReceivedQty,
PPOD.RejectedQty,
PPOH.Freight
FROM Purchasing.PurchaseOrderHeader PPOH
JOIN Purchasing.PurchaseOrderDetail PPOD
ON PPOH.PurchaseOrderID=PPOD.PurchaseOrderID
JOIN Purchasing.ShipMethod PSM
ON PSM.ShipMethodID=PPOH.ShipMethodID
JOIN Production.Product PP
ON PPOD.ProductID=PP.ProductID) AS x
ON y.PurchaseOrderDetailID=x.PurchaseOrderDetailID
FOR JSON AUTO;
'''
)

purchaseJSON = Cleaning(purchaseQuery).filterQuery() 

purchaseDataframe = makeDataFrame(purchaseJSON).conversion() 

purchaseDataframe[1] = '"Purchase History":[{'+purchaseDataframe[1] 
purchaseDataframe[12] = purchaseDataframe[12]+']}' 

duplicateRowsPurchase = purchaseDataframe[purchaseDataframe.duplicated([0])]

purchaseDataframe = purchaseDataframe[~purchaseDataframe.index.isin(duplicateRowsPurchase.index)]

makeFile(purchaseDataframe, "PurchasingCollection").saveFile()

fin = open("PurchasingCollection.js","rt")
data = fin.read()
data = data.replace('!',',')
data = data.replace('\/','/')
fin = open("PurchasingCollection.js","wt")
fin.write(data)
fin.close()

makeFile(duplicateRowsPurchase, "updatePurchasingCollection").saveFile()

fin = open("updatePurchasingCollection.js","rt")
data = fin.read()
data = data.replace('!',',')
data = data.replace('\/','/')
data = data.replace('db.updatePurchasingCollection.insertMany([','')
data = data.replace('}]}])','}]})')
data = data.replace('{"PurchaseOrderID"','db.PurchasingCollection.update({"PurchaseOrderID"')
data = data.replace(', "Purchase History":[','}, {$push :{"Purchase History":')
data = data.replace('}]}','}}})')
data = data.replace(', db.PurchasingCollection.update','''
                    
db.PurchasingCollection.update''')
data = data.replace('}}}))','}}})')
fin = open("updatePurchasingCollection.js","wt")
fin.write(data)
fin.close()











