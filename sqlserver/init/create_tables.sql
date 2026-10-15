USE banking_db;
GO

-- banking_db.dbo.mcc_codes definition
CREATE TABLE banking_db.dbo.mcc_codes (
	mcc_id int NOT NULL,
	description nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT PK_mcc_codes PRIMARY KEY (mcc_id)
);
GO

-- banking_db.dbo.users definition
CREATE TABLE banking_db.dbo.users (
	id int NOT NULL,
	current_age int NULL,
	retirement_age int NULL,
	birth_year int NULL,
	birth_month int NULL,
	gender nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	address nvarchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	latitude nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	longitude nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	per_capita_income decimal(12,2) NULL,
	yearly_income decimal(12,2) NULL,
	total_debt decimal(12,2) NULL,
	credit_score int NULL,
	num_credit_cards int NULL,
	CONSTRAINT PK_users PRIMARY KEY (id)
);
GO

-- banking_db.dbo.cards definition
CREATE TABLE banking_db.dbo.cards (
	id int NOT NULL,
	client_id int NULL,
	card_brand nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	card_type nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	card_number nvarchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	expires date NULL,
	cvv nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	has_chip nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	num_cards_issued int NULL,
	credit_limit decimal(12,2) NULL,
	acct_open_date date NULL,
	year_pin_last_changed int NULL,
	CONSTRAINT PK_cards PRIMARY KEY (id),
    CONSTRAINT FK_cards_client_id FOREIGN KEY (client_id) REFERENCES dbo.users(id) ON DELETE CASCADE
);
GO


-- banking_db.dbo.transactions definition
CREATE TABLE banking_db.dbo.transactions (
	id int NOT NULL,
	date datetime NULL,
	client_id int NULL,
	card_id int NULL,
	amount decimal(10,2) NULL,
	use_chip nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	merchant_id int NULL,
	merchant_city nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	merchant_state nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	zip nvarchar(10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	mcc int NULL,
	errors nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	CONSTRAINT PK_transactions PRIMARY KEY (id),
    CONSTRAINT FK_transactions_client_id FOREIGN KEY (client_id) REFERENCES dbo.users(id) ON DELETE NO ACTION,
    CONSTRAINT FK_transactions_card_id FOREIGN KEY (card_id) REFERENCES dbo.cards(id) ON DELETE NO ACTION,
    CONSTRAINT FK_transactions_mcc FOREIGN KEY (mcc) REFERENCES dbo.mcc_codes(mcc_id) ON DELETE NO ACTION
);
GO

