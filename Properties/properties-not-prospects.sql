

-- Sub query

select
	Client.ClientID,
	case
		when Client.NumberOfRooms > 1 then 'Property'
		When Client.ClientID in (
			'E88EDCFA-A18E-49C4-BC4E-B309544C7E96',
			'368D90D2-0557-4200-81A5-2A73EDEA0914',
			'E5862E66-BA5E-4C8E-B555-CC6C0F2FA807',
			'5BD6D5FF-2370-43A7-AB50-EA9E686A40AC',
			'4761A1DE-B97C-433F-BDEA-E8F68B71B9D4',
			'1FB496F5-1240-4F1D-8EA3-F92D9974A366',
			'B3EAA4BE-3609-4638-81B6-13884741F0B0',
			'8763F9B0-3C45-42AF-BB86-1FDC978A737D',
			'2CA80799-E44D-4B9D-BFC1-466C254781A7',
			'7FBD96E8-8F3D-4AAE-9B6C-4D6E2A86FA3F',
			'85193048-BB1C-40D5-B8BA-8B27FFB5BA08',
			'99A1CEE9-0E85-44B3-BC3A-AE58BF59FA71',
			'3E8800B3-F3FC-4D1F-A1E7-CFE9E1C00F3C',
			'55238FB9-4D8A-46F5-A480-7C59E487BCB9',
			'62980B75-9D48-44A0-8DE8-1927D4CF9AC2',
			'3AEA60D1-EF8C-47B7-BD90-1DA60B20ED49',
			'FE51D46C-F391-4683-B745-087C62D2BE49',
			'578B3883-D9D2-4A49-86B3-17AD644EA395') then 'Property'
		when TypeProperty.TypeName in ('Advertising Co','Agent','Airline','Brand','Building Owner','Church','Condo Association','Construction/Building','Consultant',
										'Developer','Equipment sales','Fitness Company','Government','HSIA Provider','Internet Reseller','Managing Company','MDU',
										'Other','Rental company','Reseller','Retail','Unsure') then 'Company'
		
		when TypeProperty.TypeName in ('Airport','Apartment','Areana / Stadium','Bed & Breakfast','Casino/ Hotel','College / University','Commercial Office Building',
										'Condo/Hotel','Condominium','Conference','Convention Ctr','Country Club','Hospital','Hotel','Hotel/Casino','Hotel/Resort',
										'Inn','Laundromat','Mall/Outlet Mall','Marina','Military Housing','Motel','Multi-Dwelling Unit','Multi-Family','Museum',
										'Office Park','Owners home','Parks/Fairgrounds','Racetrack','Resort','Resort/ Amusement Park','Resort/Hotel','Resort/Spa',
										'Restaurant/Bar','Retirement Community','RV Park','School','Senior Living','Ship/Hotel','Student Housing','Timeshare',
										'TimeShare/Hotel','Training Facility','Vacation Rental','Warehouse','Wellness Retreat','Housing') then 'Property'
		when Client.IsParentCompany = 1 then 'Company'
		when TypeProperty.TypeName in ('Individual') then 'Contact'
		else 'Unknown'
	end as 'CompanyVsProperty'
into #CompanyVsProperty
from Client
left join TypeProperty on Client.TypePropertyID = TypeProperty.TypeID
where TypeProperty.TypeName != 'Individual'
	or Typeproperty.TypeName is null



-- Properties Export
SELECT	
	
	-- TOP FORM
	Client.ClientID as 'CRM Client Id',
	Client.ClientName as 'Property Name',
	#CompanyVsProperty.CompanyVsProperty as 'Company or Property?',
	TypeClientStatus.TypeName as 'Property Status',
	--todo: TypeClientStatus mapping
	case
		when TypeProperty.TypeName in ('Advertising Co', 'Unsure', 'Other', 'Equipment Sales') then 'NULL'
		when TypeProperty.TypeName = 'Areana / Stadium' then 'Arena / Stadium'
		when TypeProperty.TypeName = 'Resort/ Amusement Park' then 'Amusement Park'
		when TypeProperty.TypeName in ('Casino/ Hotel', 'Hotel/Casino') then 'Casino'
		when TypeProperty.TypeName in ('School', 'College / University') then 'College, University, School'
		when TypeProperty.TypeName = 'Condo/Hotel' then 'Condominium'
		when TypeProperty.TypeName in ('Conference', 'Convention Ctr') then 'Conference Center'
		when TypeProperty.TypeName  = 'Owners home' then 'Home'
		when TypeProperty.TypeName  = 'Mall/Outlet Mall' then 'Mall / Outlet Mall'
		when TypeProperty.TypeName in ('Hotel/Resort', 'Resort/Hotel') then 'Hotel'
		when TypeProperty.TypeName  = 'Mall/Outlet Mall' then 'Mall / Outlet Mall'
		when TypeProperty.TypeName in ('Multi-Family', 'Housing') then 'MDU'
		when TypeProperty.TypeName  = 'Commercial Office Building' then 'Office Building'
		when TypeProperty.TypeName  = 'Parks/Fairgrounds' then 'Park'
		when TypeProperty.TypeName  = 'Restaurant/Bar' then 'Restaurant'
		when TypeProperty.TypeName  = 'RV Park' then 'RV Park / Mobile Home Park'
		when TypeProperty.TypeName  = 'Ship/Hotel' then 'Ship'
		when TypeProperty.TypeName  = 'Resort/Spa' then 'Spa'
		when TypeProperty.TypeName  = 'TimeShare/Hotel' then 'TimeShare / Hotel'
		else TypeProperty.TypeName
	end as 'Property Type',
	Client.Phone as 'Phone Number',
	Client.Phone2 as 'Alternate Phone Number',
	Client.Fax as 'Fax',
	Address.Address1 as 'Street',
	Address.Address2 as 'Street2',
	Address.City as 'City',
	Address.State as 'State',
	Address.Zip as 'Zip',
	case 
		when Address.Country in ('USA', 'US') then 'United States'
		when Address.Country = 'ABW' then 'Aruba'
		when Address.Country = 'ANTIGUA' then 'Antigua and Barbuda'
		when Address.Country = 'Brazil ' then 'Brazil'
		when Address.Country = 'BRITISH VIRGIN ISLANDS' then 'Virgin Islands (British)'
		when Address.Country = 'Dominican' then 'Dominican Republic'
		when Address.Country = 'MEX' then 'Mexico'
		when Address.Country = 'NLD' then 'Netherlands'
		when Address.Country = 'Russia' then 'Russian Federation'
		when Address.Country = 'Sierra' then 'Sierra Leone'
		when Address.Country = 'Trinidad' then 'Trinidad and Tobago'
		when Address.Country = 'UAE' then 'United Arab Emirates'
		when Address.Country = 'UNITED STATES VIRGIN ISLANDS' then 'Virgin Islands (USA)'
		else Address.Country
	end as 'Country',
	Client.Website as 'Website',
	case
		when Client.LegalName != '' and Client.LegalName is not null THEN Client.LegalName
	else Client.Clientname
	end as 'Contract Holder',
	Concat(Person.FirstName, ' ', Person.LastName) as 'Account Manager',
	isnull(TypeHotelBrand.TypeName, '') as 'Brand',
	isnull(TypeHotelSubBrand.TypeName,'') as 'Sub-brand',
	--todo: services checkboxes
	

	-- BeyondTV Page
	TypeBTVService.TypeName as 'Solution Type',
	Client.NumberOfTVs as 'Number of Installed TVs',
	TypeBTVSTBModel.TypeName as 'STB Model',
	TypeRemote.TypeName as 'Remote Model',
	Client.BTVTVModels as 'TV Models',
	isnull(TypeBTVHiboxServer.TypeName,'') as 'Hibox Server',
	isnull(TypeBTVPMS.TypeName,'') as 'PMS Type',
	Client.BTVNumberOfChannels as 'Number of TV Channels',
	Client.BTVFTGProvider as 'TV Channel Provider',
	Client.EncryptionTypeUsed as 'TV Channel Encryption Type',
	Client.BTVPropertyNotes as 'System Notes',


	-- Details Page
	isnull(Client.CustomID, Client.ImportID) as 'Brand Property Code',
	Client.PreviousNames as 'Previous Names',
	Client.NumberOfRooms as 'Total Number of Rooms',


	-- GuestCast Page
	--todo: get shelly's grid for mapping and figure out how to add that
	Client.GuestCast_TVBrands as 'TV Brand',
	Client.GuestCast_TVModels as 'TV Models',
	Client.GuestCast_RemoteModel as 'Remote Type',
	Client.InstalledBySubcontractor as 'Installed By',
	Client.GuestCast_AdditionalNotes as 'Notes',
	Client.GuestCast_TypeofSetTopBox as 'Set-top Box Type',
	Client.GuestCast_TVHDMIPort as 'Chromecast HDMI Port',
	Client.GuestCast_SSID as 'Chromecast SSID',
	Client.GuestCast_WiFiPassword as 'Chromecast SSID Passphrase',
	Client.GuestCast_GoogleAccount as 'Google Home Username',
	Client.GuestCast_GoogleAccountPassword as 'Google Home Password',
	Client.GuestCast_GoogleAccountRecoveryPhoneEmail as 'Google Home Recovery Phone / Email',
	Client.GuestCast_CastingAppID as 'Casting Receiver App ID',
	Client.GuestCast_CastingAppName as 'Casting Recieving App Name',
	Client.GuestCast_CountryCode as 'Chromecast Country Code',
	Client.GuestCast_CastingURLIP as 'Client Registration QR URL',


	--Instruction Cards page
	TypeTentCard.TypeName as 'Type',
	Client.TentIsCustomDesign as 'Custom Design',
	Client.ReplacementTentCardCost as 'Custom Design Replacement Cost',
	Client.ReplacementTentCardNotes as 'Custom Design Details',
	case
		when Client.TentTollFreeNumber is not null and Client.TentTollFreeNumber != '' then Client.TentTollFreeNumber
		when Client.CallCenterPhoneNumber is not null and Client.CallCenterPhoneNumber != '' then Client.CallCenterPhoneNumber
		else null
	end as 'Toll Free Number',


	-- Integrations Tab
	Client.TimeZone as 'Time Zone',
	Client.ExternalIDCAS as 'CAS Property ID',
	Client.NagiosUserName as 'Nagios API User',
	Client.NagiosAuthToken as 'Nagios API Key',
	Client.HasCCTransactions as 'Has Credit Card Transactions',
	Client.HasPMSTransactions as 'Has PMS Transacations',
	Client.ViperNet as 'Using ViperNet Merchant Account',
	Client.FusionAccountID as 'Fusion Merchant Account ID',


	-- Networks Tab
	TypeWifiSystem.TypeName as 'Network Infrastructure Type',
	Client.HasWiredInternet as 'In-room Wired Ports',
	Client.MonitoringName as 'Nagios Property Name',
	Client.TechSystemSummary as 'System Summary',
	Client.TechDeviceLocations as 'Device Locations',
	Client.TroubleshootingNotes as 'Troubleshooting Notes',
	Client.TechIPInfo as 'WAN IP Information',
	-- NOTE: Probably have to temporarily add these until they can get parsed.
	
	--Add fields above ISP Information Section.
	Client.TechISPInfo as 'ISP Information',
	Client.CircuitSize as 'Curcuit Size',
	
	--Add fields above Networks
	Client.ConventionDetails as 'Convention Details',
	Client.ConferenceToolURL as 'Conference Tool URL',
	Client.ConventionPortalURL as 'Conference Portal URL',
	Client.ConventionNotes as 'Notes',

	--Add fields above Authentication
	Client.AuthPropertyNotes as 'Authentication Notes',
	Client.PortalPageURL as 'Captive Portal URl',
	Client.LandingPage as 'Landing Page URL',
	Client.HasPMSIntegration as 'PMS Integration',


	-- Chatter Fields
	Client.CreatedDate as 'Chatter -- Created Date',
	Client.LastUpdatedDate as 'Chatter -- Last Updated Date',
	Client.LastUpdatedByID as 'Chatter -- Last Updated By'

  FROM Client

  -- Company vs property
  left join #CompanyVsProperty on Client.ClientID = #CompanyVsProperty.ClientID

  -- Type Joins
  left join TypeProperty on Client.TypePropertyID = TypeProperty.TypeID
  left join TypeHotelBrand on Client.TypeHotelBrandID = TypeHotelBrand.TypeID
  left join TypeHotelSubBrand on Client.TypeHotelSubBrandID = TypeHotelSubBrand.TypeID
  left join TypeClientStatus on Client.ClientStatusID = TypeClientStatus.TypeID
  left join TypeLeadSourceNew on Client.LeadSourceID = TypeLeadSourceNew.OldTypeID
  
  -- BeyondTV Type Joins
  left join TypeRemote on Client.TypeRemoteID = TypeRemote.TypeID
  left join TypeBTVSTBModel on Client.TypeBTVSTBModelID = TypeBTVSTBModel.TypeID
  left join TypeBTVService on Client.BTVTypeID = TypeBTVService.TypeID
  left join TypeBTVPMS on Client.TypeBTVPMSID = TypeBTVPMS.TypeID
  left join TypeBTVHiBoxServer on Client.TypeBTVHiBoxServerID = TypeBTVHiBoxServer.TypeID
  
  -- Instruction Card Joins
  left join TypeTentCard on Client.TentTypeID = TypeTentCard.TypeID

  -- Network Tab joins
  left join TypeWifiSystem on Client.TypeWifiSystemID = TypeWiFiSystem.TypeID

  -- Other Joins
  left join Address on Client.AddressID = Address.AddressID
  left join Person on Client.AccountManagerID = Person.PersonID
    
  where 
	(Client.IsDeleted = 0 or client.IsDeleted is null)
	and (Client.IsProspect = 0 or Client.IsProspect is null)
	AND (TypeProperty.Typename != 'Individual' or TypeProperty.TypeName is null)
	AND (IsAdminCompany != 1 or IsAdminCompany is null)
	AND Client.ClientID not in (
		'84D50DDA-A529-40EB-8018-9A0601079669',
		'169E03C1-5A0B-43D2-8525-DB8F23EBFD98',
		'BEE04CF6-1133-445C-AF4E-7F78D74EE0AF',
		'99D4E45A-AA92-40D7-848C-7BC033FB5C64')
	AND #CompanyVsProperty.CompanyVsProperty = 'Property'

  order by TypeClientStatus.TypeName;

 DROP TABLE #CompanyVsProperty;