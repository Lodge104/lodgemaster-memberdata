import json
import boto3
import os
import csv
import codecs
import sys
import urllib3
import io

http = urllib3.PoolManager()

dynamodb = boto3.resource('dynamodb')

client = boto3.client('dynamodb')

tableName = os.environ['table']

p1 = client.get_item(
    Key={
        'name': {
            'S': 'XSRF-TOKEN',
        },
    },
    TableName='lodge104-keys',
)
p2 = client.get_item(
    Key={
        'name': {
            'S': 'ai_session',
        },
    },
    TableName='lodge104-keys',
)
p3 = client.get_item(
    Key={
        'name': {
            'S': 'OA.LM.Lodge.Auth',
        },
    },
    TableName='lodge104-keys',
) 
p4 = client.get_item(
    Key={
        'name': {
            'S': 'ai_user',
        },
    },
    TableName='lodge104-keys',
) 
url = "https://lodgemaster-client.oa-bsa.org/api/members/grid/export"

body = 'exportFormat=3&groupSeparateTabs=true&exportAllColumns=true&exportOalmId=true&gridParams=%7B%22%24top%22%3A100%2C%22%24select%22%3A%22Positions%2CEmailPrimaryBounced%2CAddressPrimaryBounced%2CPhonePrimaryBounced%2CBrotherhoodDate%2CBrotherhoodEligibleDate%2CBsaRegistrationActive%2CBsaPersonId%2CBsaLastCheckDate%2CChapter%2CAddressPrimaryCity%2CDateOfBirth%2CEmailPrimaryAddress%2CFirstName%2CGender%2CHealthMedicalDietaryRestrictions%2CHealthMedicalHasMedicalCondition%2CHealthMedicalHasMedicine%2CHealthMedicalOtherNotes%2CHealthMedicalHasOtherAllergies%2CHealthMedicalValidFromDate%2CLastName%2CPositionLec%2CLevel%2CDuesYear%2CMiddleName%2COrdealDate%2CPhonePrimaryNumber%2CAddressPrimaryState%2CAddressPrimaryStreet1%2CAddressPrimaryStreet2%2CUnit%2CUnitNumber%2CUnitType%2CVigilEligibleDate%2CVigilInductionDate%2CVigilNameEnglish%2CVigilNameTranslated%2CYouthProtectionCompliant%2CYouthProtectionDate%2CYouthAdult%2CAddressPrimaryZipCode%2COalmId%22%2C%22%24filter%22%3A%22(BsaPersonId%20ne%20null)%20and%20(DateOfDeath%20eq%20null)%20and%20(Level%20ne%20\'Non-Member\')%22%2C%22%24count%22%3A%22true%22%2C%22duesIncludeYears%22%3A%222049%2C2048%2C2047%2C2046%2C2045%2C2044%2C2043%2C2042%2C2041%2C2040%2C2039%2C2038%2C2037%2C2036%2C2035%2C2034%2C2033%2C2032%2C2031%2C2030%2C2029%2C2028%2C2027%2C2026%2C2025%2C2024%2C2023%2C2022%2C2021%2C2020%2C2019%2C2018%2C2017%2C2016%2C2015%2C2014%2C2013%2C2012%2C2011%2C2010%2C2009%2C2008%2C2007%2C2006%2C2005%2C2004%2C2003%2C2002%2C2001%2C2000%2C1999%2C1998%2C1997%2C1993%2C1982%2C1981%2C1974%22%2C%22duesIncludeNonPaid%22%3Atrue%7D&columns=%5B%7B%22id%22%3A%22Positions%22%2C%22index%22%3A8%7D%2C%7B%22id%22%3A%22EmailPrimaryBounced%22%2C%22index%22%3A29%7D%2C%7B%22id%22%3A%22AddressPrimaryBounced%22%2C%22index%22%3A35%7D%2C%7B%22id%22%3A%22PhonePrimaryBounced%22%2C%22index%22%3A27%7D%2C%7B%22id%22%3A%22BrotherhoodDate%22%2C%22index%22%3A14%7D%2C%7B%22id%22%3A%22BrotherhoodEligibleDate%22%2C%22index%22%3A15%7D%2C%7B%22id%22%3A%22BsaRegistrationActive%22%2C%22index%22%3A1%7D%2C%7B%22id%22%3A%22BsaPersonId%22%2C%22index%22%3A0%7D%2C%7B%22id%22%3A%22BsaLastCheckDate%22%2C%22index%22%3A2%7D%2C%7B%22id%22%3A%22Chapter%22%2C%22index%22%3A10%7D%2C%7B%22id%22%3A%22AddressPrimaryCity%22%2C%22index%22%3A32%7D%2C%7B%22id%22%3A%22DateOfBirth%22%2C%22index%22%3A21%7D%2C%7B%22id%22%3A%22EmailPrimaryAddress%22%2C%22index%22%3A28%7D%2C%7B%22id%22%3A%22FirstName%22%2C%22index%22%3A5%7D%2C%7B%22id%22%3A%22Gender%22%2C%22index%22%3A23%7D%2C%7B%22id%22%3A%22HealthMedicalDietaryRestrictions%22%2C%22index%22%3A36%7D%2C%7B%22id%22%3A%22HealthMedicalHasMedicalCondition%22%2C%22index%22%3A37%7D%2C%7B%22id%22%3A%22HealthMedicalHasMedicine%22%2C%22index%22%3A38%7D%2C%7B%22id%22%3A%22HealthMedicalOtherNotes%22%2C%22index%22%3A40%7D%2C%7B%22id%22%3A%22HealthMedicalHasOtherAllergies%22%2C%22index%22%3A39%7D%2C%7B%22id%22%3A%22HealthMedicalValidFromDate%22%2C%22index%22%3A41%7D%2C%7B%22id%22%3A%22LastName%22%2C%22index%22%3A7%7D%2C%7B%22id%22%3A%22PositionLec%22%2C%22index%22%3A20%7D%2C%7B%22id%22%3A%22Level%22%2C%22index%22%3A12%7D%2C%7B%22id%22%3A%22DuesYear%22%2C%22index%22%3A9%7D%2C%7B%22id%22%3A%22MiddleName%22%2C%22index%22%3A6%7D%2C%7B%22id%22%3A%22OrdealDate%22%2C%22index%22%3A13%7D%2C%7B%22id%22%3A%22PhonePrimaryNumber%22%2C%22index%22%3A26%7D%2C%7B%22id%22%3A%22AddressPrimaryState%22%2C%22index%22%3A33%7D%2C%7B%22id%22%3A%22AddressPrimaryStreet1%22%2C%22index%22%3A30%7D%2C%7B%22id%22%3A%22AddressPrimaryStreet2%22%2C%22index%22%3A31%7D%2C%7B%22id%22%3A%22Unit%22%2C%22index%22%3A11%7D%2C%7B%22id%22%3A%22UnitNumber%22%2C%22index%22%3A25%7D%2C%7B%22id%22%3A%22UnitType%22%2C%22index%22%3A24%7D%2C%7B%22id%22%3A%22VigilEligibleDate%22%2C%22index%22%3A16%7D%2C%7B%22id%22%3A%22VigilInductionDate%22%2C%22index%22%3A17%7D%2C%7B%22id%22%3A%22VigilNameEnglish%22%2C%22index%22%3A18%7D%2C%7B%22id%22%3A%22VigilNameTranslated%22%2C%22index%22%3A19%7D%2C%7B%22id%22%3A%22YouthProtectionCompliant%22%2C%22index%22%3A3%7D%2C%7B%22id%22%3A%22YouthProtectionDate%22%2C%22index%22%3A4%7D%2C%7B%22id%22%3A%22YouthAdult%22%2C%22index%22%3A22%7D%2C%7B%22id%22%3A%22AddressPrimaryZipCode%22%2C%22index%22%3A34%7D%5D'
headers = {
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
'Origin': 'https://lodgemaster-client.oa-bsa.org',
'Content-Type': 'application/x-www-form-urlencoded',
'Cookie': p1['Item']['name']['S'] + '=' + p1['Item']['value']['S'] + '; ' + p2['Item']['name']['S'] + '=' + p2['Item']['value']['S'] + '; ' + p3['Item']['name']['S'] + '=' + p3['Item']['value']['S'] + '; ' + p4['Item']['name']['S'] + '=' + p4['Item']['value']['S'] + ';'
}

def lambda_handler(event, context):
   #get() does not store in memory
   try:
      table = dynamodb.Table(tableName)
   except Exception as error:
      print(error)
      print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")

   batch_size = 100
   batch = []

   #DictReader is a generator; not stored in memory
   obj = http.request('POST', url, headers=headers, body=body)
   content = obj.data.decode("utf-8")
   for row in csv.DictReader(io.StringIO(content)):
      if len(batch) >= batch_size:
         write_to_dynamo(batch)
         batch.clear()

      batch.append(row)

   if batch:
      write_to_dynamo(batch)

   return {
      'statusCode': 200,
      'body': json.dumps('Uploaded to DynamoDB Table')
   }


def write_to_dynamo(rows):
   try:
      table = dynamodb.Table(tableName)
   except Exception as error:
      print(error)
      print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")

   try:
      with table.batch_writer() as batch:
         for i in range(len(rows)):
            batch.put_item(
               Item=rows[i]
            )
   except Exception as error:
      print(error)
      print("Error executing batch_writer")