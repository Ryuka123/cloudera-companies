--
-- Schema Create
--

CREATE TABLE IF NOT EXISTS Company (
	CompanyName STRING,
	CompanyNumber STRING,
	RegAddressCareOf STRING,
	RegAddressPOBox STRING,
	RegAddressAddressLine1 STRING,
	RegAddressAddressLine2 STRING,
	RegAddressPostTown STRING,
	RegAddressCounty STRING,
	RegAddressCountry STRING,
	RegAddressPostCode STRING,
	CompanyCategory STRING,
	CompanyStatus STRING,
	CountryOfOrigin STRING,
	DissolutionDate STRING,
	IncorporationDate STRING,
	AccountsAccountRefDay STRING,
	AccountsAccountRefMonth STRING,
	AccountsNextDueDate STRING,
	AccountsLastMadeUpDate STRING,
	AccountsAccountCategory STRING,
	ReturnsNextDueDate STRING,
	ReturnsLastMadeUpDate STRING,
	MortgagesNumMortCharges STRING,
	MortgagesNumMortOutstanding STRING,
	MortgagesNumMortPartSatisfied STRING,
	MortgagesNumMortSatisfied STRING,
	SICCodeSicText1 STRING,
	SICCodeSicText2 STRING,
	SICCodeSicText3 STRING,
	SICCodeSicText4 STRING,
	LimitedPartnershipsNumGenPartners STRING,
	LimitedPartnershipsNumLimPartners STRING,
	URI STRING,
	PreviousName1CONDATE STRING,
	PreviousName1CompanyName STRING,
	PreviousName2CONDATE STRING,
	PreviousName2CompanyName STRING,
	PreviousName3CONDATE STRING,
	PreviousName3CompanyName STRING,
	PreviousName4CONDATE STRING,
	PreviousName4CompanyName STRING,
	PreviousName5CONDATE STRING,
	PreviousName5CompanyName STRING,
	PreviousName6CONDATE STRING,
	PreviousName6CompanyName STRING,
	PreviousName7CONDATE STRING,
	PreviousName7CompanyName STRING,
	PreviousName8CONDATE STRING,
	PreviousName8CompanyName STRING,
	PreviousName9CONDATE STRING,
	PreviousName9CompanyName STRING,
	PreviousName10CONDATE STRING,
	PreviousName10CompanyName STRING
)
COMMENT 'Companies registered at UK Companies House'
PARTITIONED BY (
	Year STRING,
	Month STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS SEQUENCEFILE;
