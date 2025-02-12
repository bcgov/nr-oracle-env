/*
Not currently in use... but may be useful in the future.

Contains sample code that could be used for test cases.
*/

CREATE EXTENSION postgis;

CREATE database spar;

\c spar;

CREATE SCHEMA IF NOT EXISTS spar;

CREATE TABLE IF NOT EXISTS spar.user_profile (
  user_id    VARCHAR(70) NOT NULL,
  dark_theme BOOLEAN DEFAULT FALSE,
  CONSTRAINT user_pk
    primary key(user_id)
);
CREATE TABLE IF NOT EXISTS spar.favourite_activity (
  id          SERIAL,
  user_id     VARCHAR(70) NOT NULL,
  activity    VARCHAR(50) NOT NULL,
  highlighted BOOLEAN DEFAULT FALSE,
  enabled     BOOLEAN DEFAULT TRUE,
  CONSTRAINT favourite_activity_pk
    primary key(id)
);
create table spar.seedlot (
  seedlot_number                  varchar(5) not null,
  seedlot_status_code             varchar(3) not null,
  applicant_client_number         varchar(8),
  applicant_locn_code             varchar(2),
  applicant_email_address         varchar(100),
  vegetation_code                 varchar(8),
  genetic_class_code              varchar(1),
  seedlot_source_code             varchar(3),
  to_be_registrd_ind              boolean,
  bc_source_ind                   boolean,
  collection_client_number        varchar(8),
  collection_locn_code            varchar(2),
  collection_start_date           timestamp,
  collection_end_date             timestamp,
  no_of_containers                decimal(6, 2),
  vol_per_container               decimal(6, 2),
  clctn_volume                    decimal(6, 2),
  seedlot_comment                 varchar(2000),
  interm_strg_client_number       varchar(8),
  interm_strg_locn_code           varchar(2),
  interm_strg_st_date             timestamp,
  interm_strg_end_date            timestamp,
  interm_facility_code            varchar(3),
  female_gametic_mthd_code        varchar(4),
  male_gametic_mthd_code          varchar(4),
  controlled_cross_ind            boolean,
  biotech_processes_ind           boolean,
  pollen_contamination_ind        boolean,
  pollen_contamination_pct        int,
  contaminant_pollen_bv           decimal(4, 1),
  pollen_contamination_mthd_code  varchar(4),
  total_parent_trees              int,
  smp_success_pct                 int,
  effective_pop_size              decimal(5, 1),
  tested_parent_tree_cont_pct     decimal(6, 2),
  coancestry                      decimal(20, 10),
  smp_parents_outside             int,
  non_orchard_pollen_contam_pct   int,
  extractory_client_number        varchar(8),
  extractory_locn_code            varchar(2),
  extraction_st_date              timestamp,
  extraction_end_date             timestamp,
  storage_client_number           varchar(8),
  storage_locn_code               varchar(2),
  temporary_storage_start_date    timestamp,
  temporary_storage_end_date      timestamp,
  declared_userid                 varchar(30),
  declared_timestamp              timestamp,
  entry_userid                    varchar(30) not null,
  entry_timestamp                 timestamp not null,
  update_userid                   varchar(30) not null,
  update_timestamp                timestamp not null,
  revision_count                  int not null,
  constraint seedlot_pk
    primary key(seedlot_number)
);
create table spar.seedlot_collection_method (
  seedlot_number              varchar(5) not null,
  cone_collection_method_code varchar(30) not null,
  cone_collection_method_desc varchar(400),
  entry_userid                varchar(30) not null,
  entry_timestamp             timestamp not null,
  update_userid               varchar(30) not null,
  update_timestamp            timestamp not null,
  revision_count              int not null,
  constraint seedlot_collection_method_pk
    primary key(seedlot_number, cone_collection_method_code),
  constraint seedlot_coll_met_seedlot_fk
    foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);
create table spar.seedlot_orchard (
  seedlot_number    varchar(5) not null,
  orchard_id        varchar(3) not null,
  primary_ind       boolean not null,
  entry_userid      varchar(30) not null,
  entry_timestamp   timestamp not null,
  update_userid     varchar(30) not null,
  update_timestamp  timestamp not null,
  revision_count		int not null,
  constraint seedlot_orchard_pk
    primary key (seedlot_number, orchard_id),
  constraint seedlot_orchard_seedlot_fk
    foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);

create table spar.seedlot_owner_quantity (
  seedlot_number          varchar(5) not null,
  owner_client_number     varchar(8) not null,
  owner_locn_code         varchar(2) not null,
  original_pct_owned      decimal(4, 1) not null,
  original_pct_rsrvd      decimal(4, 1) not null,
  original_pct_srpls      decimal(4, 1) not null,
  method_of_payment_code  varchar(3),
  spar_fund_srce_code     varchar(3),
  entry_userid            varchar(30) not null,
  entry_timestamp         timestamp not null,
  update_userid           varchar(30) not null,
  update_timestamp        timestamp not null,
  revision_count          int not null,
  constraint seedlot_owner_quantity_pk
    primary key (seedlot_number, owner_client_number, owner_locn_code),
  constraint seedlot_owner_qty_seedlot_fk
    foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);
create table spar.seedlot_parent_tree (
    seedlot_number                varchar(5) not null,
    parent_tree_id                int not null,
    cone_count                    decimal(20, 10) not null,
    pollen_count                  decimal(20, 10) not null,
    smp_success_pct               int,
    non_orchard_pollen_contam_pct int,
    entry_userid                  varchar(30) not null,
    entry_timestamp               timestamp not null,
    update_userid                 varchar(30) not null,
    update_timestamp              timestamp not null,
    revision_count                int not null,
	constraint seedlot_parent_tree_pk
		primary key(seedlot_number, parent_tree_id),
	constraint seedlot_parent_tree_seedlot_fk
		foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);
create table spar.seedlot_parent_tree_gen_qlty (
    seedlot_number        varchar(5) not null,
    parent_tree_id        int not null,
    genetic_type_code     varchar(2) not null,
    genetic_worth_code    varchar(3) not null,
    genetic_quality_value decimal(4, 1) not null,
    estimated_ind         boolean,
    untested_ind          boolean,
    entry_userid          varchar(30) not null,
    entry_timestamp       timestamp not null,
    update_userid         varchar(30) not null,
    update_timestamp      timestamp not null,
    revision_count        int,
    constraint seedlot_parent_tree_gen_qlt_pk
        primary key(seedlot_number, parent_tree_id, genetic_type_code, genetic_worth_code),
    constraint sl_ptree_genqly_sl_ptree_fk
        foreign key(seedlot_number, parent_tree_id) references spar.seedlot_parent_tree(seedlot_number, parent_tree_id)
);
create table spar.seedlot_genetic_worth (
    seedlot_number        varchar(5) not null,
    genetic_worth_code    varchar(3) not null,
    genetic_quality_value decimal(4, 1) not null,
    entry_userid          varchar(30) not null,
    entry_timestamp       timestamp not null,
    update_userid         varchar(30) not null,
    update_timestamp      timestamp not null,
    revision_count        int not null,
    constraint seedlot_genetic_worth_pk
        primary key(seedlot_number, genetic_worth_code),
    constraint seedlot_genet_worth_seedlot_fk
        foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);
create table spar.smp_mix (
    seedlot_number 	varchar(5) not null,
    parent_tree_id 	int not null,
    amount_of_material	int not null,
    proportion		decimal(20,10),
    entry_userid	varchar(30) not null,
    entry_timestamp	timestamp not null,
    update_userid	varchar(30) not null,
    update_timestamp	timestamp not null,
    revision_count	int not null,
    constraint smp_mix_pk
        primary key(seedlot_number, parent_tree_id),
    constraint smp_mix_seedlot_fk
        foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);
create table spar.smp_mix_gen_qlty (
    seedlot_number 		varchar(5) not null,
    parent_tree_id 		int not null,
    genetic_type_code 		varchar(2) not null,
    genetic_worth_code 		varchar(3) not null,
    genetic_quality_value 	decimal(4, 1) not null,
    estimated_ind 		boolean not null,
    entry_userid		varchar(30) not null,
    entry_timestamp		timestamp not null,
    update_userid		varchar(30) not null,
    update_timestamp		timestamp not null,
    revision_count		int not null,
    constraint smp_mix_gen_qlty_pk
        primary key(seedlot_number, parent_tree_id, genetic_type_code, genetic_worth_code),
    constraint smp_mix_gen_qlty_smp_mix_pk
        foreign key(seedlot_number, parent_tree_id) references spar.smp_mix(seedlot_number, parent_tree_id)
);
create table spar.seedlot_parent_tree_smp_mix (
    seedlot_number		varchar(5) not null,
    parent_tree_id		int not null,
    genetic_type_code		varchar(2) not null,
    genetic_worth_code		varchar(3) not null,
    genetic_quality_value	decimal(4, 1) not null,
    entry_userid		varchar(30) not null,
    entry_timestamp		timestamp not null,
    update_userid		varchar(30) not null,
    update_timestamp		timestamp not null,
    revision_count		int not null,
    constraint seedlot_parent_tree_smp_mix_pk
        primary key(seedlot_number, parent_tree_id, genetic_type_code, genetic_worth_code),
    constraint sl_ptree_smp_mix_sl_ptree_fk
        foreign key(seedlot_number, parent_tree_id) references spar.seedlot_parent_tree(seedlot_number, parent_tree_id)
);
create table spar.active_orchard_spu (
  orchard_id          varchar(3) not null,
  seed_plan_unit_id   int not null,
  active_ind boolean  not null,
  retired_ind boolean not null,
  no_spu_ind boolean  not null,
  constraint active_orchard_spu_pk
    primary key(orchard_id, seed_plan_unit_id));
insert into spar.active_orchard_spu
    (orchard_id, seed_plan_unit_id, active_ind, retired_ind, no_spu_ind)
    values ('101', 7, 'False', 'True', 'False'),
           ('102', 5, 'True', 'False', 'False'),
           ('103', 78, 'True', 'False', 'False'),
           ('105', 140, 'True', 'False', 'False'),
           ('109', 7, 'False', 'True', 'False'),
           ('111', 7, 'False', 'True', 'False'),
           ('114', 6, 'False', 'True', 'False'),
           ('115', 7, 'False', 'True', 'False'),
           ('116', 6, 'False', 'True', 'False'),
           ('118', 55, 'False', 'True', 'False'),
           ('120', 8, 'False', 'True', 'False'),
           ('121', 6, 'False', 'True', 'False'),
           ('122', 7, 'False', 'True', 'False'),
           ('123', 7, 'False', 'True', 'False'),
           ('124', 7, 'False', 'True', 'False'),
           ('126', 22, 'False', 'True', 'False'),
           ('127', 21, 'False', 'True', 'False'),
           ('128', 4, 'False', 'True', 'False'),
           ('129', 1, 'False', 'True', 'False'),
           ('130', 21, 'False', 'True', 'False'),
           ('131', 77, 'False', 'True', 'False'),
           ('132', 22, 'False', 'True', 'False'),
           ('133', 22, 'False', 'True', 'False'),
           ('134', 7, 'True', 'False', 'False'),
           ('135', 1, 'False', 'True', 'False'),
           ('136', 22, 'False', 'True', 'False'),
           ('137', 78, 'False', 'True', 'False'),
           ('138', 78, 'False', 'True', 'False'),
           ('139', 4, 'False', 'True', 'False'),
           ('140', 4, 'True', 'False', 'False'),
           ('141', 1, 'False', 'True', 'False'),
           ('142', 55, 'False', 'True', 'False'),
           ('143', 21, 'False', 'True', 'False'),
           ('145', 54, 'True', 'False', 'False'),
           ('146', 8, 'False', 'True', 'False'),
           ('147', 8, 'False', 'True', 'False'),
           ('148', 4, 'True', 'False', 'False'),
           ('149', 7, 'False', 'True', 'False'),
           ('150', 22, 'False', 'True', 'False'),
           ('151', 68, 'False', 'True', 'False'),
           ('152', 4, 'True', 'False', 'False'),
           ('153', 4, 'False', 'True', 'False'),
           ('154', 7, 'True', 'False', 'False'),
           ('155', 4, 'False', 'True', 'False'),
           ('156', 22, 'False', 'True', 'False'),
           ('157', 55, 'False', 'True', 'False'),
           ('158', 4, 'False', 'True', 'False'),
           ('159', -1, 'False', 'True', 'True'),
           ('160', 1, 'False', 'True', 'False'),
           ('161', -1, 'False', 'True', 'True'),
           ('162', 7, 'False', 'True', 'False'),
           ('164', 78, 'False', 'True', 'False'),
           ('165', 22, 'False', 'True', 'False'),
           ('166', 7, 'True', 'False', 'False'),
           ('168', 7, 'False', 'True', 'False'),
           ('169', 7, 'False', 'True', 'False'),
           ('170', 22, 'True', 'False', 'False'),
           ('171', 4, 'False', 'True', 'False'),
           ('172', 55, 'True', 'False', 'False'),
           ('173', 54, 'False', 'True', 'False'),
           ('174', 54, 'False', 'True', 'False'),
           ('175', 54, 'True', 'False', 'False'),
           ('176', 22, 'False', 'True', 'False'),
           ('177', 7, 'False', 'True', 'False'),
           ('178', 77, 'False', 'True', 'False'),
           ('179', 22, 'False', 'True', 'False'),
           ('180', 1, 'False', 'True', 'False'),
           ('181', 8, 'True', 'False', 'False'),
           ('182', 22, 'False', 'True', 'False'),
           ('183', 7, 'True', 'False', 'False'),
           ('184', 4, 'True', 'False', 'False'),
           ('185', 7, 'False', 'True', 'False'),
           ('186', 4, 'False', 'True', 'False'),
           ('187', 21, 'True', 'False', 'False'),
           ('188', 22, 'False', 'True', 'False'),
           ('189', 4, 'False', 'True', 'False'),
           ('190', 4, 'True', 'False', 'False'),
           ('191', 78, 'False', 'True', 'False'),
           ('192', 55, 'True', 'False', 'False'),
           ('193', 4, 'False', 'True', 'False'),
           ('194', 78, 'False', 'True', 'False'),
           ('195', 55, 'True', 'False', 'False'),
           ('196', 21, 'True', 'False', 'False'),
           ('197', 7, 'True', 'False', 'False'),
           ('198', 4, 'True', 'False', 'False'),
           ('199', 7, 'True', 'False', 'False'),
           ('201', 35, 'False', 'True', 'False'),
           ('202', 35, 'False', 'True', 'False'),
           ('203', 45, 'False', 'True', 'False'),
           ('204', 29, 'False', 'True', 'False'),
           ('205', 68, 'False', 'True', 'False'),
           ('207', 57, 'True', 'False', 'False'),
           ('208', 57, 'True', 'False', 'False'),
           ('209', 68, 'False', 'True', 'False'),
           ('210', 68, 'False', 'True', 'False'),
           ('211', 68, 'True', 'False', 'False'),
           ('212', 90, 'True', 'False', 'False'),
           ('213', 72, 'True', 'False', 'False'),
           ('215', 68, 'False', 'True', 'False'),
           ('216', 68, 'False', 'True', 'False'),
           ('218', 35, 'True', 'False', 'False'),
           ('219', 29, 'True', 'False', 'False'),
           ('220', 45, 'True', 'False', 'False'),
           ('221', 45, 'True', 'False', 'False'),
           ('222', 45, 'True', 'False', 'False'),
           ('223', 35, 'True', 'False', 'False'),
           ('224', 35, 'False', 'True', 'False'),
           ('225', 14, 'True', 'False', 'False'),
           ('226', 16, 'True', 'False', 'False'),
           ('228', 29, 'True', 'False', 'False'),
           ('229', 57, 'True', 'False', 'False'),
           ('231', 10, 'True', 'False', 'False'),
           ('232', 16, 'True', 'False', 'False'),
           ('233', 14, 'True', 'False', 'False'),
           ('234', 29, 'True', 'False', 'False'),
           ('236', 45, 'True', 'False', 'False'),
           ('237', 45, 'True', 'False', 'False'),
           ('238', 35, 'True', 'False', 'False'),
           ('239', 67, 'True', 'False', 'False'),
           ('240', 29, 'True', 'False', 'False'),
           ('241', 35, 'True', 'False', 'False'),
           ('242', 57, 'True', 'False', 'False'),
           ('243', 57, 'True', 'False', 'False'),
           ('244', 45, 'True', 'False', 'False'),
           ('245', 29, 'True', 'False', 'False'),
           ('246', 35, 'True', 'False', 'False'),
           ('247', 68, 'True', 'False', 'False'),
           ('249', 57, 'True', 'False', 'False'),
           ('250', 57, 'True', 'False', 'False'),
           ('301', 89, 'False', 'True', 'False'),
           ('302', 63, 'False', 'True', 'False'),
           ('304', 60, 'False', 'True', 'False'),
           ('305', 89, 'True', 'False', 'False'),
           ('306', 63, 'True', 'False', 'False'),
           ('307', 41, 'False', 'True', 'False'),
           ('308', 51, 'False', 'True', 'False'),
           ('310', 50, 'True', 'False', 'False'),
           ('311', 51, 'True', 'False', 'False'),
           ('313', 41, 'True', 'False', 'False'),
           ('321', 13, 'True', 'False', 'False'),
           ('324', 12, 'True', 'False', 'False'),
           ('332', 24, 'True', 'False', 'False'),
           ('333', 23, 'True', 'False', 'False'),
           ('334', 23, 'False', 'True', 'False'),
           ('335', 85, 'True', 'False', 'False'),
           ('336', 11, 'True', 'False', 'False'),
           ('337', 41, 'True', 'False', 'False'),
           ('338', 51, 'True', 'False', 'False'),
           ('339', 50, 'True', 'False', 'False'),
           ('341', 62, 'True', 'False', 'False'),
           ('342', 74, 'True', 'False', 'False'),
           ('343', 73, 'True', 'False', 'False'),
           ('345', -1, 'True', 'False', 'True'),
           ('346', 135, 'True', 'False', 'False'),
           ('347', 41, 'True', 'False', 'False'),
           ('349', 40, 'True', 'False', 'False'),
           ('350', 51, 'True', 'False', 'False'),
           ('351', 85, 'True', 'False', 'False'),
           ('352', 35, 'True', 'False', 'False'),
           ('353', 80, 'True', 'False', 'False'),
           ('354', 142, 'True', 'False', 'False'),
           ('355', 141, 'True', 'False', 'False'),
           ('356', 60, 'True', 'False', 'False'),
           ('357', 140, 'True', 'False', 'False'),
           ('358', 50, 'True', 'False', 'False'),
           ('401', 7, 'False', 'True', 'False'),
           ('403', 54, 'True', 'False', 'False'),
           ('405', 7, 'True', 'False', 'False'),
           ('406', 6, 'True', 'False', 'False'),
           ('407', -1, 'True', 'False', 'True'),
           ('408', 4, 'True', 'False', 'False'),
           ('409', 130, 'True', 'False', 'False'),
           ('410', 130, 'True', 'False', 'False'),
           ('411', 29, 'True', 'False', 'False'),
           ('412', 29, 'True', 'False', 'False'),
           ('413', 29, 'True', 'False', 'False'),
           ('414', 85, 'True', 'False', 'False'),
           ('415', 85, 'True', 'False', 'False'),
           ('416', 54, 'True', 'False', 'False'),
           ('417', 29, 'True', 'False', 'False'),
           ('418', 29, 'True', 'False', 'False'),
           ('607', 85, 'False', 'True', 'False'),
           ('609', 85, 'True', 'False', 'False'),
           ('610', 56, 'False', 'True', 'False'),
           ('611', 67, 'False', 'True', 'False'),
           ('612', 60, 'False', 'True', 'False'),
           ('620', 57, 'False', 'True', 'False'),
           ('800', 78, 'True', 'False', 'False'),
           ('801', 4, 'True', 'False', 'False'),
           ('802', 78, 'False', 'True', 'False'),
           ('803', 78, 'True', 'False', 'False'),
           ('804', 78, 'False', 'True', 'False'),
           ('805', 78, 'True', 'False', 'False'),
           ('806', 78, 'False', 'True', 'False'),
           ('807', 78, 'False', 'True', 'False'),
           ('808', 78, 'True', 'False', 'False'),
           ('810', -1, 'False', 'True', 'True'),
           ('811', 4, 'True', 'False', 'False'),
           ('813', -1, 'True', 'False', 'True'),
           ('814', 103, 'False', 'True', 'False'),
           ('815', -1, 'True', 'False', 'True'),
           ('816', 103, 'False', 'True', 'False'),
           ('818', 103, 'False', 'True', 'False'),
           ('819', 103, 'False', 'True', 'False'),
           ('820', -1, 'True', 'False', 'True'),
           ('821', 103, 'False', 'True', 'False'),
           ('822', 68, 'False', 'True', 'False'),
           ('823', 68, 'False', 'True', 'False'),
           ('824', 103, 'True', 'False', 'False'),
           ('825', 103, 'False', 'True', 'False'),
           ('826', 103, 'False', 'True', 'False'),
           ('829', 78, 'False', 'True', 'False'),
           ('830', 104, 'True', 'False', 'False'),
           ('831', 108, 'False', 'True', 'False'),
           ('833', 78, 'False', 'True', 'False'),
           ('834', -1, 'False', 'True', 'True'),
           ('835', 7, 'False', 'True', 'False'),
           ('836', 78, 'True', 'False', 'False'),
           ('837', -1, 'False', 'True', 'True'),
           ('838', 7, 'False', 'True', 'False'),
           ('839', 68, 'False', 'True', 'False'),
           ('840', 55, 'False', 'True', 'False'),
           ('841', 103, 'False', 'True', 'False'),
           ('842', 78, 'True', 'False', 'False'),
           ('989', -1, 'False', 'True', 'True'),
           ('990', -1, 'True', 'False', 'True'),
           ('991', 7, 'False', 'True', 'False'),
           ('992', 22, 'True', 'False', 'False'),
           ('993', 4, 'True', 'False', 'False'),
           ('994', -1, 'True', 'False', 'True'),
           ('995', -1, 'True', 'False', 'True'),
           ('996', 7, 'True', 'False', 'False'),
           ('997', -1, 'True', 'False', 'True'),
           ('998', 54, 'True', 'False', 'False'),
           ('999', 85, 'True', 'False', 'False'),
           ('110', 6, 'False', 'True', 'False'),
           ('110', 7, 'False', 'True', 'False'),
           ('144', 55, 'False', 'False', 'False'),
           ('144', 108, 'True', 'False', 'False'),
           ('163', 21, 'False', 'True', 'False'),
           ('163', 22, 'False', 'True', 'False'),
           ('206', 67, 'True', 'False', 'False'),
           ('206', 68, 'False', 'False', 'False'),
           ('214', 67, 'False', 'False', 'False'),
           ('214', 68, 'True', 'False', 'False'),
           ('217', 56, 'False', 'True', 'False'),
           ('217', 57, 'False', 'True', 'False'),
           ('230', 29, 'True', 'False', 'False'),
           ('230', 43, 'False', 'False', 'False'),
           ('235', 29, 'False', 'False', 'False'),
           ('235', 35, 'True', 'False', 'False'),
           ('235', 45, 'False', 'False', 'False'),
           ('303', 73, 'False', 'True', 'False'),
           ('303', 74, 'False', 'True', 'False'),
           ('340', 38, 'True', 'False', 'False'),
           ('340', 39, 'False', 'False', 'False'),
           ('614', 67, 'False', 'True', 'True'),
           ('614', 68, 'False', 'True', 'True'),
           ('809', 103, 'False', 'True', 'False'),
           ('809', 104, 'False', 'True', 'False');
-- ConeCollectionMethodEnum
create table spar.cone_collection_method_list (
	cone_collection_method_code serial not null,
	description varchar(120) not null,
	effective_date date not null,
	expiry_date date not null,
	update_timestamp timestamp default current_timestamp,
	constraint cone_collection_method_list_pk primary key(cone_collection_method_code)
);

comment on table spar.cone_collection_method_list is 'A list of valid Cone based Collection Method Codes.';

comment on column spar.cone_collection_method_list.cone_collection_method_code is 'A code describing various Cone Collection Methods.';

comment on column spar.cone_collection_method_list.description is 'A description for the affiliated code.';

comment on column spar.cone_collection_method_list.effective_date is 'The effective date the code is in effect';

comment on column spar.cone_collection_method_list.expiry_date is 'The date the code expires on.';

comment on column spar.cone_collection_method_list.update_timestamp is 'The date and time of the last update.';

insert into
	spar.cone_collection_method_list (description, effective_date, expiry_date)
values
	(
		'Aerial raking',
		'1905-01-01',
		'9999-12-31'
	),
	(
		'Aerial clipping/topping',
		'1905-01-01',
		'9999-12-31'
	),
	(
		'Felled trees',
		'1905-01-01',
		'9999-12-31'
	),
	(
		'Climbing',
		'1905-01-01',
		'9999-12-31'
	),
	(
		'Squirrel cache',
		'1905-01-01',
		'9999-12-31'
	),
	(
		'Ground, Ladder and/or Hydraulic Lift',
		'1905-01-01',
		'9999-12-31'
	),
	(
		'Unknown',
		'1905-01-01',
		'9999-12-31'
	),
	(
		'Squirrel harvesting/dropping',
		'2013-12-03',
		'9999-12-31'
	);

alter table
	spar.seedlot_collection_method
alter column
	cone_collection_method_code type integer using cone_collection_method_code :: integer;

alter table
	spar.seedlot_collection_method
add
	constraint seedlot_cone_col_met_code_fk foreign key(cone_collection_method_code) references spar.cone_collection_method_list(cone_collection_method_code);

-- GeneticClassEnum
create table spar.genetic_class_list (
	genetic_class_code varchar(1) not null,
	description varchar(120) not null,
	effective_date date not null,
	expiry_date date not null,
	update_timestamp timestamp default current_timestamp,
	constraint genetic_class_list_pk primary key(genetic_class_code)
);

comment on table spar.genetic_class_list is 'A list of valid Genetic Class Codes that indicate if a lot is collected from Parent Trees in an Orchard or from a natural stand.';

comment on column spar.genetic_class_list.genetic_class_code is 'A code describing various Genetic Classes.';

comment on column spar.genetic_class_list.description is 'A description for the affiliated code.';

comment on column spar.genetic_class_list.effective_date is 'The effective date the code is in effect';

comment on column spar.genetic_class_list.expiry_date is 'The date the code expires on.';

comment on column spar.genetic_class_list.update_timestamp is 'The date and time of the last update.';

insert into
	spar.genetic_class_list (
		genetic_class_code,
		description,
		effective_date,
		expiry_date,
		update_timestamp
	)
values
	(
		'A',
		'Orchard Seed or Cuttings',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'B',
		'Natural Stand Seed or Cuttings',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	);

alter table
	spar.seedlot
add
	constraint seedlot_genetic_class_code_fk foreign key(genetic_class_code) references spar.genetic_class_list(genetic_class_code);

-- GeneticWorthEnum
create table spar.genetic_worth_list (
	genetic_worth_code varchar(3) not null,
	description varchar(120) not null,
	effective_date date not null,
	expiry_date date not null,
	update_timestamp timestamp default current_timestamp,
	constraint genetic_worth_list_pk primary key(genetic_worth_code)
);

comment on table spar.genetic_worth_list is 'A list of valid codes that describe the genetic trait(s) of Parent Trees, Seedlots and Vegetative Lots.';

comment on column spar.genetic_worth_list.genetic_worth_code is 'A code describing various Genetic Worths.';

comment on column spar.genetic_worth_list.description is 'A description for the affiliated code.';

comment on column spar.genetic_worth_list.effective_date is 'The effective date the code is in effect';

comment on column spar.genetic_worth_list.expiry_date is 'The date the code expires on.';

comment on column spar.genetic_worth_list.update_timestamp is 'The date and time of the last update.';

insert into
	spar.genetic_worth_list (
		genetic_worth_code,
		description,
		effective_date,
		expiry_date,
		update_timestamp
	)
values
	(
		'D',
		'Relative Wood Density',
		'1998-07-15',
		'2018-05-29',
		current_timestamp
	),
	(
		'G',
		'Growth and Volume',
		'1905-01-01',
		'2018-05-29',
		current_timestamp
	),
	(
		'GVO',
		'Volume Growth',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'WVE',
		'Wood Velocity Measures',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'WWD',
		'Wood quality',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'R',
		'Pest Resistance',
		'1905-01-01',
		'2018-05-29',
		current_timestamp
	),
	(
		'M',
		'Major Gene Resistance',
		'2006-09-06',
		'2018-05-29',
		current_timestamp
	),
	(
		'WDU',
		'Wood durability',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'AD',
		'Animal browse resistance (deer)',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'DFS',
		'Disease resistance for Dothistroma needle blight (Dothistroma septosporum)',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'DFU',
		'Disease resistance for Redcedar leaf blight (Didymascella thujina)',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'DFW',
		'Disease resistance for Swiss needle cast (Phaeocryptopus gaumanni)',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'DSB',
		'Disease resistance for white pine blister rust (Cronartium ribicola)',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'DSC',
		'Disease resistance for Commandra blister rust (Cronartium comandrae)',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'DSG',
		'Disease resistance for Western gall rust (Endocronartium harknessii)',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	),
	(
		'IWS',
		'Spruce terminal weevil (Pissodes strobi)',
		'2018-05-29',
		'9999-12-31',
		current_timestamp
	);

alter table
	spar.seedlot_parent_tree_gen_qlty
add
	constraint seedlot_pt_genqlt_genwor_cd_fk foreign key(genetic_worth_code) references spar.genetic_worth_list(genetic_worth_code);

alter table
	spar.seedlot_genetic_worth
add
	constraint seedlot_genetic_worth_code_fk foreign key(genetic_worth_code) references spar.genetic_worth_list(genetic_worth_code);

alter table
	spar.smp_mix_gen_qlty
add
	constraint smp_mix_gen_qlty_gen_wor_cd_fk foreign key(genetic_worth_code) references spar.genetic_worth_list(genetic_worth_code);

alter table
	spar.seedlot_parent_tree_smp_mix
add
	constraint seedlot_pt_smpmix_genwor_cd_fk foreign key(genetic_worth_code) references spar.genetic_worth_list(genetic_worth_code);

-- MaleFemaleMethodologyEnum
create table spar.gametic_methodology_list (
	gametic_methodology_code varchar(3) not null,
	description varchar(120) not null,
	female_methodology_ind boolean not null,
	pli_species_ind boolean not null,
	effective_date date not null,
	expiry_date date not null,
	update_timestamp timestamp not null,
	constraint gametic_methodology_list_pk primary key(gametic_methodology_code)
);

comment on table spar.gametic_methodology_list is 'A list of valid codes indicating the methods used in a seed Orchard to estimate the female or male component of gamete contributions to a Seedlot.';

comment on column spar.gametic_methodology_list.gametic_methodology_code is 'Code that describes the gametic contribution method code for a seedlot';

comment on column spar.gametic_methodology_list.description is 'A description for the affiliated code.';

comment on column spar.gametic_methodology_list.female_methodology_ind is 'A flag that indicates if the methodology is female or not.';

comment on column spar.gametic_methodology_list.pli_species_ind is 'A flag that indicates if the methodology can be used by a PLI species or not';

comment on column spar.gametic_methodology_list.effective_date is 'The effective date the code is in effect';

comment on column spar.gametic_methodology_list.expiry_date is 'The date the code expires on.';

comment on column spar.gametic_methodology_list.update_timestamp is 'The date and time of the last update.';

insert into
	spar.gametic_methodology_list (
		gametic_methodology_code,
		description,
		female_methodology_ind,
		pli_species_ind,
		effective_date,
		expiry_date,
		update_timestamp
	)
values
	(
		'F1',
		'Visual Estimate',
		true,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'F2',
		'Measured Cone Volume',
		true,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'F3',
		'Cone Weight',
		true,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'F4',
		'Cone Number from Weight',
		true,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'F5',
		'Cone Number from Standard Volume',
		true,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'F6',
		'Sample of Seeds',
		true,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'F7',
		'Filled Seeds',
		true,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'F8',
		'Ramet Proportion by Clone',
		true,
		true,
		'2015-06-03',
		'9999-12-31',
		current_timestamp
	),
	(
		'F9',
		'Ramet Proportion by Age and Expected Production',
		true,
		true,
		'2015-06-03',
		'9999-12-31',
		current_timestamp
	),
	(
		'M1',
		'Portion of Ramets in Orchard',
		false,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'M2',
		'Pollen Volume Estimate by Partial Survey',
		false,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'M3',
		'Pollen Volume Estimate by 100% Survey',
		false,
		false,
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'M4',
		'Ramet Proportion by Clone',
		false,
		true,
		'2015-06-03',
		'9999-12-31',
		current_timestamp
	),
	(
		'M5',
		'Ramet Proportion by Age and Expected Production',
		false,
		true,
		'2015-06-03',
		'9999-12-31',
		current_timestamp
	);

alter table
	spar.seedlot
add
	constraint seedlot_female_game_mthd_cd_fk foreign key(female_gametic_mthd_code) references spar.gametic_methodology_list(gametic_methodology_code);

alter table
	spar.seedlot
add
	constraint seedlot_male_game_mthd_cd_fk foreign key(male_gametic_mthd_code) references spar.gametic_methodology_list(gametic_methodology_code);

-- PaymentMethodEnum
create table spar.method_of_payment_list (
	method_of_payment_code varchar(3) not null,
	description varchar(120) not null,
	default_method_ind boolean default null,
	effective_date date not null,
	expiry_date date not null,
	update_timestamp timestamp not null,
	constraint method_of_payment_list_pk primary key(method_of_payment_code),
	constraint only_one_default_method unique (default_method_ind)
);

comment on table spar.method_of_payment_list is 'A list of valid Method of Payment Codes.';

comment on column spar.method_of_payment_list.method_of_payment_code is 'A code describing various Method of Payments.';

comment on column spar.method_of_payment_list.description is 'A description for the affiliated code.';

comment on column spar.method_of_payment_list.default_method_ind is 'A flag that determines if the method is default.';

comment on column spar.method_of_payment_list.effective_date is 'The effective date the code is in effect.';

comment on column spar.method_of_payment_list.expiry_date is 'The date the code expires on.';

comment on column spar.method_of_payment_list.update_timestamp is 'The date and time of the last update.';

insert into
	spar.method_of_payment_list (
		method_of_payment_code,
		description,
		default_method_ind,
		effective_date,
		expiry_date,
		update_timestamp
	)
values
	(
		'CLA',
		'Invoice to MOF Client Account',
		null,
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'CSH',
		'Cash Sale',
		null,
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'ITC',
		'Invoice to Client Address',
		true,
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'ITO',
		'Invoice to Other Address',
		null,
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'JV',
		'Journal Voucher',
		null,
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'NC',
		'Non-chargeable',
		null,
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	);

alter table
	spar.seedlot_owner_quantity
add
	constraint seedlot_own_qnt_meth_pay_cd_fk foreign key(method_of_payment_code) references spar.method_of_payment_list(method_of_payment_code);

-- SeedlotSourceEnum
create table spar.seedlot_source_list (
	seedlot_source_code varchar(3) not null,
	description varchar(120) not null,
	effective_date date not null,
	expiry_date date not null,
	update_timestamp timestamp default current_timestamp,
	constraint seedlot_source_list_pk primary key(seedlot_source_code)
);

comment on table spar.seedlot_source_list is 'A list of valid Lot Source Codes.';

comment on column spar.seedlot_source_list.seedlot_source_code is 'A code to indicate if an orchard seedlot is from tested parent trees, untested or custom.';

comment on column spar.seedlot_source_list.description is 'A description for the affiliated code.';

comment on column spar.seedlot_source_list.effective_date is 'The effective date the code is in effect';

comment on column spar.seedlot_source_list.expiry_date is 'The date the code expires on.';

comment on column spar.seedlot_source_list.update_timestamp is 'The date and time of the last update.';

insert into
	spar.seedlot_source_list (
		seedlot_source_code,
		description,
		effective_date,
		expiry_date,
		update_timestamp
	)
values
	(
		'CUS',
		'Custom Lot',
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'TPT',
		'Tested Parent Trees',
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	),
	(
		'UPT',
		'Untested Parent Trees',
		'2005-07-25',
		'9999-12-31',
		current_timestamp
	);

alter table
	spar.seedlot
add
	constraint seedlot_seedlot_source_code_fk foreign key(seedlot_source_code) references spar.seedlot_source_list(seedlot_source_code);

-- SeedlotStatusEnum
create table spar.seedlot_status_list (
	seedlot_status_code varchar(3) not null,
	description varchar(120) not null,
	effective_date date not null,
	expiry_date date not null,
	update_timestamp timestamp default current_timestamp,
	constraint seedlot_status_list_pk primary key(seedlot_status_code)
);

comment on table spar.seedlot_status_list is 'A list of valid Seedlot based Status Codes.';

comment on column spar.seedlot_status_list.seedlot_status_code is 'A code describing various Seedlot Statuses.';

comment on column spar.seedlot_status_list.description is 'A description for the affiliated code.';

comment on column spar.seedlot_status_list.effective_date is 'The effective date the code is in effect';

comment on column spar.seedlot_status_list.expiry_date is 'The date the code expires on.';

comment on column spar.seedlot_status_list.update_timestamp is 'The date and time of the last update.';

insert into
	spar.seedlot_status_list (
		seedlot_status_code,
		description,
		effective_date,
		expiry_date,
		update_timestamp
	)
values
	(
		'APP',
		'Approved',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'CAN',
		'Cancelled',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'COM',
		'Complete',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'INC',
		'Incomplete',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'PND',
		'Pending',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'EXP',
		'Expired',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	),
	(
		'SUB',
		'Submitted',
		'1905-01-01',
		'9999-12-31',
		current_timestamp
	);

alter table
	spar.seedlot
add
	constraint seedlot_seedlot_status_code_fk foreign key(seedlot_status_code) references spar.seedlot_status_list(seedlot_status_code);

alter table
	spar.seedlot_orchard drop column primary_ind;

alter table
	spar.seedlot_collection_method
alter column
	cone_collection_method_desc type varchar(50);

alter table
	spar.seedlot_collection_method rename column cone_collection_method_desc to cone_collection_method_other_desc;
-- seedlot
comment on table spar.seedlot is 'A quantity of seed of a particular species and quality collected from either a natural stand or seed Orchard in a given time period.';
comment on column spar.seedlot.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot.seedlot_status_code is 'A code which represents the current status of a lot.';
comment on column spar.seedlot.applicant_client_number is 'A sequentially assigned number which uniquely identifies a Ministry client (applicant).';
comment on column spar.seedlot.applicant_locn_code is 'A code to uniquely identify, within each client (applicant), the addresses of different divisions or locations at which the client operates. The location code is sequentially assigned starting with "00" for the client´s permanent address.';
comment on column spar.seedlot.applicant_email_address is 'Email address of client applying to register the Seedlot';
comment on column spar.seedlot.vegetation_code is 'A code which represents a species of tree or brush.';
comment on column spar.seedlot.genetic_class_code is 'A code which represents the Genetic Quality of material (seed or cuttings). "A" class represents superior Orchard produced seed or cuttings. "B" class represents naturally collected seed or cuttings.';
comment on column spar.seedlot.seedlot_source_code is 'A code to indicate if an Orchard Seedlot is from tested Parent Trees, untested or custom.';
comment on column spar.seedlot.to_be_registrd_ind is 'An indicator which represents whether a Seedlot is intended to be registered for crown land reforestation use ("Y" yes) or not ("N" no).';
comment on column spar.seedlot.bc_source_ind is 'Indicates whether the source of the Seedlot is within British Columbia';
comment on column spar.seedlot.collection_client_number is 'A sequentially assigned number which uniquely identifies a Ministry client (collection).';
comment on column spar.seedlot.collection_locn_code is 'A code to uniquely identify, within each client (collection), the addresses of different divisions or locations at which the client operates. The location code is sequentially assigned starting with "00" for the client´s permanent address.';
comment on column spar.seedlot.collection_start_date is 'The actual start date (year, month, and day) that the cones (source for seedlots) were collected.';
comment on column spar.seedlot.collection_end_date is 'The actual end date (year, month, and day) that the cones (source for seedlots) were collected.';
comment on column spar.seedlot.no_of_containers is 'The number of containers (sacks of cones) that were collected.';
comment on column spar.seedlot.vol_per_container is 'The volume of cones, in hectolitres, that were collected per container (as reported on the cone collection form).';
comment on column spar.seedlot.clctn_volume is 'A code which represents the number of trees that the Seedlot was collected from.';
comment on column spar.seedlot.seedlot_comment is 'A free format text field used to enter general comments for a Seedlot.';
comment on column spar.seedlot.interm_strg_client_number is 'A sequentially assigned number which uniquely identifies a Ministry client (interm storage).';
comment on column spar.seedlot.interm_strg_locn_code is 'A code to uniquely identify, within each client (interm storage), the addresses of different divisions or locations at which the client operates. The location code is sequentially assigned starting with "00" for the client´s permanent address.';
comment on column spar.seedlot.interm_strg_st_date is 'The actual start date (year, month, and day) when the cone was stored during interim storage.';
comment on column spar.seedlot.interm_strg_end_date is 'The actual end date (year, month, and day) when the cone was stored during interim storage.';
comment on column spar.seedlot.interm_facility_code is 'A code which represents the type of facility where the seed was stored during interim storage.';
comment on column spar.seedlot.female_gametic_mthd_code is 'Code that describes the female gametic contribution method code for a Seedlot.';
comment on column spar.seedlot.male_gametic_mthd_code is 'Code that describes the male gametic contribution method code for a Seedlot.';
comment on column spar.seedlot.controlled_cross_ind is 'Indicates whether the lot was produced through controlled crosses.';
comment on column spar.seedlot.biotech_processes_ind is 'Indicates if biotechnological processes been used to produce this lot.';
comment on column spar.seedlot.pollen_contamination_ind is 'Indicates if pollen contamination was present in the seed Orchard';
comment on column spar.seedlot.pollen_contamination_pct is 'The proportion of contaminant pollen present in the seed Orchard.';
comment on column spar.seedlot.contaminant_pollen_bv is 'The estimated Breeding Value of the contaminant pollen in an Orchard.';
comment on column spar.seedlot.pollen_contamination_mthd_code is 'Code that describes the pollen contamination method.';
comment on column spar.seedlot.total_parent_trees is 'The total number of parents contributing to the Seedlot.';
comment on column spar.seedlot.smp_success_pct is 'The estimated success (percent) of the supplemental mass pollination mix on the Parent Trees in the Orchard.';
comment on column spar.seedlot.effective_pop_size is 'The calculated number of parents contributing to the Seedlot, representing the genetic variety.';
comment on column spar.seedlot.tested_parent_tree_cont_pct is 'Percentage of Parent Trees contributing to the Seedlot that were tested.';
comment on column spar.seedlot.coancestry is 'This value was taken from the SeedlotCertificationTemplate.xls (2005 version with Coancestry - sheet reference "PiPj  Cij"!P1) as used by the Tree Seed Centre to calculate Genetic Worth, Effective Population Size and Collection Geography prior to this fun.';
comment on column spar.seedlot.smp_parents_outside is 'The number of Parent Trees from outside an Orchard used in the supplemental mass pollination mix.';
comment on column spar.seedlot.non_orchard_pollen_contam_pct is 'Non-orchard pollen contamination (%) for the Seedlot.';
comment on column spar.seedlot.extractory_client_number is 'A sequentially assigned number which uniquely identifies a Ministry client (extractory).';
comment on column spar.seedlot.extractory_locn_code is 'A code to uniquely identify, within each client (extractory), the addresses of different divisions or locations at which the client operates. The location code is sequentially assigned starting with "00" for the client´s permanent address.';
comment on column spar.seedlot.extraction_st_date is 'The actual start date (year, month, and day) when the seed was extracted from the cones.';
comment on column spar.seedlot.extraction_end_date is 'The actual end date (year, month, and day) when the seed was extracted from the cones.';
comment on column spar.seedlot.storage_client_number is 'A sequentially assigned number which uniquely identifies a Ministry client (storage).';
comment on column spar.seedlot.storage_locn_code is 'A code to uniquely identify, within each client (storage), the addresses of different divisions or locations at which the client operates. The location code is sequentially assigned starting with "00" for the client´s permanent address.';
comment on column spar.seedlot.temporary_storage_start_date is 'Commencement date of temporary Seedlot storage.';
comment on column spar.seedlot.temporary_storage_end_date is 'End date of Seedlot temporary storage.';
comment on column spar.seedlot.declared_userid is 'The userid of the individual that declares the following: - that the information is true and correct - that they are the owner of the lot or are authorized by the owner(s) to submit the Seedlot application.';
comment on column spar.seedlot.declared_timestamp is 'The date and time that the following were declared by the declarer: - that the information is true and correct - that they are the owner of the lot or are authorized by the owner(s) to submit the Seedlot application.';
comment on column spar.seedlot.entry_userid is 'The userid of the individual that entered the Seedlot.';
comment on column spar.seedlot.entry_timestamp is 'The time and date a Seedlot was entered onto the system.';
comment on column spar.seedlot.update_userid is 'The userid of the individual that changed the Seedlot.';
comment on column spar.seedlot.update_timestamp is 'The time and date a Seedlot was last updated on the system.';
comment on column spar.seedlot.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- seedlot_collection_method
comment on table spar.seedlot_collection_method is 'The collection methods in which the cones of a Seedlot were collected.';
comment on column spar.seedlot_collection_method.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_collection_method.cone_collection_method_code is 'A code representing the method in which the cones were collected.';
comment on column spar.seedlot_collection_method.cone_collection_method_other_desc is 'A description of the method in which the cones were collected - used only when the user selects the option "Other".';
comment on column spar.seedlot_collection_method.entry_userid is 'The userid of the individual that entered the Seedlot collection method.';
comment on column spar.seedlot_collection_method.entry_timestamp is 'The time and date a Seedlot collection method was entered onto the system.';
comment on column spar.seedlot_collection_method.update_userid is 'The userid of the individual that changed the Seedlot collection method.';
comment on column spar.seedlot_collection_method.update_timestamp is 'The time and date a Seedlot collection method was last updated on the system.';
comment on column spar.seedlot_collection_method.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- seedlot_owner_quantity
comment on table spar.seedlot_owner_quantity is 'A Seedlot may be owned by more than one owner. Each owner owns a specific quantity of seed which may be reserved for the use of that owner and/or surplus and available for use by others.';
comment on column spar.seedlot_owner_quantity.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_owner_quantity.owner_client_number is 'A sequentially assigned number which uniquely identifies a Ministry client (owner).';
comment on column spar.seedlot_owner_quantity.owner_locn_code is 'A code to uniquely identify, within each client (owner), the addresses of different divisions or locations at which the client operates. The location code is sequentially assigned starting with "00" for the client´s permanent address.';
comment on column spar.seedlot_owner_quantity.original_pct_owned is 'The original percentage of seed that was owned by a client.';
comment on column spar.seedlot_owner_quantity.original_pct_rsrvd is 'The original percentage of seed from an owner´s portion of a Seedlot that has been declared reserved.';
comment on column spar.seedlot_owner_quantity.original_pct_srpls is 'The original percentage of seed from an owner´s portion of a Seedlot that has been declared surplus.';
comment on column spar.seedlot_owner_quantity.method_of_payment_code is 'A code which represents the method of payment for services where payment is required.';
comment on column spar.seedlot_owner_quantity.spar_fund_srce_code is 'A code which represents the source funds for payment of the request.';
comment on column spar.seedlot_owner_quantity.entry_userid is 'The userid of the individual that entered the Seedlot owner quantity.';
comment on column spar.seedlot_owner_quantity.entry_timestamp is 'The time and date a Seedlot owner quantity was entered onto the system.';
comment on column spar.seedlot_owner_quantity.update_userid is 'The userid of the individual that changed the Seedlot owner quantity.';
comment on column spar.seedlot_owner_quantity.update_timestamp is 'The time and date a Seedlot owner quantity was last updated on the system.';
comment on column spar.seedlot_owner_quantity.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- seedlot_orchard
comment on table spar.seedlot_orchard is 'Orchard location where cuttings or A class seed is produced.';
comment on column spar.seedlot_orchard.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_orchard.orchard_id is 'A unique identifier which is assigned to a location where cuttings or A class seed is produced.';
comment on column spar.seedlot_orchard.entry_userid is 'The userid of the individual that entered the Seedlot Orchard.';
comment on column spar.seedlot_orchard.entry_timestamp is 'The time and date a Seedlot Orchard was entered onto the system.';
comment on column spar.seedlot_orchard.update_userid is 'The userid of the individual that changed the Seedlot Orchard.';
comment on column spar.seedlot_orchard.update_timestamp is 'The time and date a Seedlot Orchard was last updated on the system.';
comment on column spar.seedlot_orchard.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- seedlot_parent_tree
comment on table spar.seedlot_parent_tree is 'The contribution of Parent Trees (with their cone and pollen quantity of each Parent Tree) to an Orchard Seedlot (Genetic Class = "A")';
comment on column spar.seedlot_parent_tree.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_parent_tree.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.seedlot_parent_tree.cone_count is 'The number of cones counted or estimated from each Parent Tree for a Seedlot.';
comment on column spar.seedlot_parent_tree.pollen_count is 'The amount of pollen counted or estimated for each Parent Tree in the Seedlot.';
comment on column spar.seedlot_parent_tree.smp_success_pct is 'The estimated success (percent) of the supplemental mass pollination mix on the Parent Trees in the Orchard.';
comment on column spar.seedlot_parent_tree.non_orchard_pollen_contam_pct is 'Non-orchard pollen contamination (%).';
comment on column spar.seedlot_parent_tree.entry_userid is 'The userid of the individual that entered the Seedlot Parent Tree.';
comment on column spar.seedlot_parent_tree.entry_timestamp is 'The time and date a Seedlot Parent Tree was entered onto the system.';
comment on column spar.seedlot_parent_tree.update_userid is 'The userid of the individual that changed the Seedlot Parent Tree.';
comment on column spar.seedlot_parent_tree.update_timestamp is 'The time and date a Seedlot Parent Tree was last updated on the system.';
comment on column spar.seedlot_parent_tree.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- seedlot_parent_tree_gen_qlty
comment on table spar.seedlot_parent_tree_gen_qlty is 'The Genetic Worth value(s) for an Orchard Seedlot (Genetic Class = "A") calculated from the Genetic Quality (Breeding Values) of the Parent Trees contributing to the Seedlot.';
comment on column spar.seedlot_parent_tree_gen_qlty.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_parent_tree_gen_qlty.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.seedlot_parent_tree_gen_qlty.genetic_type_code is 'Describes the comparative measure of genetic value for a specific genetic trait of a Parent Tree.  Examples are BV (Breeding Value) and CV (Clonal Value).';
comment on column spar.seedlot_parent_tree_gen_qlty.genetic_worth_code is 'A code describing various Genetic Worths.';
comment on column spar.seedlot_parent_tree_gen_qlty.genetic_quality_value is 'The Genetic Quality value based on the test assessment for a Parent Tree from a test no. and series.';
comment on column spar.seedlot_parent_tree_gen_qlty.estimated_ind is 'Indicates whether or not the test result was estimated. A Genetic Quality value of 2.0 is estimated for Untested Parent Trees (i.e. Parent Trees without an Area of Use) or for Tested Parent Trees (i.e. Parent Trees with an Area of Use) without a Genetic.';
comment on column spar.seedlot_parent_tree_gen_qlty.untested_ind is 'Indicates whether or not the test result was estimated for an untested Parent Tree.';
comment on column spar.seedlot_parent_tree_gen_qlty.entry_userid is 'The userid of the individual that entered the Seedlot Parent Tree Genetic Quality.';
comment on column spar.seedlot_parent_tree_gen_qlty.entry_timestamp is 'The time and date a Seedlot Parent Tree Genetic Quality was entered onto the system.';
comment on column spar.seedlot_parent_tree_gen_qlty.update_userid is 'The userid of the individual that changed the Seedlot Parent Tree Genetic Quality.';
comment on column spar.seedlot_parent_tree_gen_qlty.update_timestamp is 'The time and date a Seedlot Parent Tree Genetic Quality was last updated on the system.';
comment on column spar.seedlot_parent_tree_gen_qlty.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- seedlot_genetic_worth
comment on table spar.seedlot_genetic_worth is 'A subclassification of Genetic Quality for "A" class Seedlots.';
comment on column spar.seedlot_genetic_worth.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_genetic_worth.genetic_worth_code is 'A code which represents a subclassification of Genetic Quality for "A" class seedlots.';
comment on column spar.seedlot_genetic_worth.genetic_quality_value is 'The rating for a subclassification of Genetic Quality for "A" class seedlots.';
comment on column spar.seedlot_genetic_worth.entry_userid is 'The userid of the individual that entered the Seedlot Genetic Worth.';
comment on column spar.seedlot_genetic_worth.entry_timestamp is 'The time and date a Seedlot Genetic Worth was entered onto the system.';
comment on column spar.seedlot_genetic_worth.update_userid is 'The userid of the individual that changed the Seedlot Genetic Worth.';
comment on column spar.seedlot_genetic_worth.update_timestamp is 'The time and date a Seedlot Genetic Worth was last updated on the system.';
comment on column spar.seedlot_genetic_worth.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- smp_mix
comment on table spar.smp_mix is 'A table listing the individual Parent Trees that contributed to the Supplemental Mass Pollination mix of an Orchard Seedlot (Genetic Class = "A").';
comment on column spar.smp_mix.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.smp_mix.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.smp_mix.amount_of_material is 'The amount of Parent Tree material used for calculating the proportion of mix. Usually a volume recorded in mL.';
comment on column spar.smp_mix.proportion is 'The proportion of each Parent Tree material used for calculating SMP mix.';
comment on column spar.smp_mix.entry_userid is 'The userid of the individual that entered the SMP mix.';
comment on column spar.smp_mix.entry_timestamp is 'The time and date a SMP mix was entered onto the system.';
comment on column spar.smp_mix.update_userid is 'The userid of the individual that changed the SMP mix.';
comment on column spar.smp_mix.update_timestamp is 'The time and date a SMP mix was last updated on the system.';
comment on column spar.smp_mix.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- smp_mix_gen_qlty
comment on table spar.smp_mix_gen_qlty is 'The calculated Genetic Worth value(s) for the Supplemental Mass Pollination mix that contributed to an Orchard Seedlot (Genetic Class = "A") the Parent Trees contributing to a Seedlot.';
comment on column spar.smp_mix_gen_qlty.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.smp_mix_gen_qlty.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.smp_mix_gen_qlty.genetic_type_code is 'Describes the comparative measure of genetic value for a specific genetic trait of a Parent Tree. Examples are BV (Breeding Value) and CV (Clonal Value).';
comment on column spar.smp_mix_gen_qlty.genetic_worth_code is 'A code describing various Genetic Worths.';
comment on column spar.smp_mix_gen_qlty.genetic_quality_value is 'The Genetic Quality value based on the test assessment for a Parent Tree from a test no. and series.';
comment on column spar.smp_mix_gen_qlty.entry_userid is 'The userid of the individual that entered the SMP mix Genetic Quality.';
comment on column spar.smp_mix_gen_qlty.entry_timestamp is 'The time and date a SMP mix Genetic Quality was entered onto the system.';
comment on column spar.smp_mix_gen_qlty.update_userid is 'The userid of the individual that changed the SMP mix Genetic Quality.';
comment on column spar.smp_mix_gen_qlty.update_timestamp is 'The time and date a SMP mix Genetic Quality was last updated on the system.';
comment on column spar.smp_mix_gen_qlty.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- seedlot_parent_tree_smp_mix
comment on table spar.seedlot_parent_tree_smp_mix is 'Supplemental Mass Polination for a given Seedlot, Parent Tree, Genetic Type and Genetic worth combination.';
comment on column spar.seedlot_parent_tree_smp_mix.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_parent_tree_smp_mix.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.seedlot_parent_tree_smp_mix.genetic_type_code is 'Describes the comparative measure of genetic value for a specific genetic trait of a Parent Tree. Examples are BV (Breeding Value) and CV (Clonal Value).';;
comment on column spar.seedlot_parent_tree_smp_mix.genetic_worth_code is 'A code describing various Genetic Worths.';;
comment on column spar.seedlot_parent_tree_smp_mix.genetic_quality_value is 'The Genetic Quality value based on the test assessment for a Parent Tree from a test no. and series.';
comment on column spar.seedlot_parent_tree_smp_mix.entry_userid is 'The userid of the individual that entered the Parent Tree SMP mix';
comment on column spar.seedlot_parent_tree_smp_mix.entry_timestamp is 'The time and date a Parent Tree SMP mix was entered onto the system.';
comment on column spar.seedlot_parent_tree_smp_mix.update_userid is 'The userid of the individual that changed the Parent Tree SMP mix.';
comment on column spar.seedlot_parent_tree_smp_mix.update_timestamp is 'The time and date a Parent Tree SMP mix was last updated on the system.';
comment on column spar.seedlot_parent_tree_smp_mix.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- active_orchard_spu
comment on table spar.active_orchard_spu is 'It is used to control which orchard can be used in the Seedlot registration process and also to indicate which legacy seed plan unit id should to be used.';
comment on column spar.active_orchard_spu.orchard_id is 'A unique identifier which is assigned to a location where cuttings or A class seed is produced.';
comment on column spar.active_orchard_spu.seed_plan_unit_id is 'A unique identifier which is assigned to a legacy Seed Planning Unit. It is used for historical porpuse and to link data to Oracle database. A -1 indicates that the orchard has no SPU associated with it.';
comment on column spar.active_orchard_spu.active_ind is 'Indicates if the combined orchard and SPU are the ones to be used and are active.';
comment on column spar.active_orchard_spu.retired_ind is 'Indicates retired orchards that should only be used to show legacy information.';
comment on column spar.active_orchard_spu.no_spu_ind is 'Indicates a new orchard that has not a SPU assigned yet.';
/*
* For update timestamps
* Alter table with column update_timestamp to use default current_timestamp instead of not null
* Tables that end with '_list' does not need to be altered as they are done in V16
*/
alter table
  spar.seedlot
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.seedlot_collection_method
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.seedlot_genetic_worth
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.seedlot_orchard
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.seedlot_owner_quantity
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.seedlot_parent_tree
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.seedlot_parent_tree_gen_qlty
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.seedlot_parent_tree_smp_mix
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.smp_mix
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.smp_mix_gen_qlty
alter column
  update_timestamp set default current_timestamp;

alter table
  spar.favourite_activity drop column enabled;

alter table
  spar.favourite_activity
add
  entry_timestamp timestamp default current_timestamp;

alter table
  spar.favourite_activity
add
  update_timestamp timestamp default current_timestamp;

alter table
  spar.seedlot_source_list
add
  default_source_ind boolean default null;

alter table
  spar.seedlot_source_list
add
  constraint only_one_default_source unique (default_source_ind);

update
  spar.seedlot_source_list
set
  description = 'Custom Seedlot'
where
  seedlot_source_code = 'CUS';

update
  spar.seedlot_source_list
set
  default_source_ind = true
where
  seedlot_source_code = 'TPT';
-- All tables in the script need to be recreated as there is no other way to add columns in a specific position. Once a table is being used by the application, it will only be possible to add columns at the end of the table.
-- Moving tested_parent_tree_cont_pct column from seedlot table to seedlot_genetic_worth table.
alter table spar.seedlot drop column tested_parent_tree_cont_pct;
alter table spar.seedlot drop column coancestry;

alter table spar.seedlot rename column storage_client_number to temporary_strg_client_number;
alter table spar.seedlot rename column storage_locn_code to temporary_strg_locn_code;
alter table spar.seedlot rename column temporary_storage_start_date to temporary_strg_start_date;
alter table spar.seedlot rename column temporary_storage_end_date to temporary_strg_end_date;

alter table spar.seedlot add column interm_strg_locn varchar(55);
comment on column spar.seedlot.interm_strg_locn is 'The location where the tree seed was stored during interim storage. Can be used if users did not enter a registered client data.';

drop table spar.seedlot_genetic_worth;

create table spar.seedlot_genetic_worth (
  seedlot_number                 varchar(5) not null,
  genetic_worth_code             varchar(3) not null,
  genetic_quality_value          decimal(4, 1) not null,
  tested_parent_tree_cont_pct    decimal(6, 2),
  entry_userid                   varchar(30) not null,
  entry_timestamp                timestamp not null,
  update_userid                  varchar(30) not null,
  update_timestamp               timestamp not null,
  revision_count                 int not null,
  constraint seedlot_genetic_worth_pk
    primary key(seedlot_number, genetic_worth_code),
  constraint seedlot_genet_worth_seedlot_fk
    foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);

comment on table spar.seedlot_genetic_worth is 'A subclassification of Genetic Quality for "A" class Seedlots.';
comment on column spar.seedlot_genetic_worth.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_genetic_worth.genetic_worth_code is 'A code which represents a subclassification of Genetic Quality for "A" class seedlots.';
comment on column spar.seedlot_genetic_worth.genetic_quality_value is 'The rating for a subclassification of Genetic Quality for "A" class seedlots.';
comment on column spar.seedlot_genetic_worth.tested_parent_tree_cont_pct is 'Percentage of parent trees that were tested for the trait and contributed to the seedlot';
comment on column spar.seedlot_genetic_worth.entry_userid is 'The userid of the individual that entered the Seedlot Genetic Worth.';
comment on column spar.seedlot_genetic_worth.entry_timestamp is 'The time and date a Seedlot Genetic Worth was entered onto the system.';
comment on column spar.seedlot_genetic_worth.update_userid is 'The userid of the individual that changed the Seedlot Genetic Worth.';
comment on column spar.seedlot_genetic_worth.update_timestamp is 'The time and date a Seedlot Genetic Worth was last updated on the system.';
comment on column spar.seedlot_genetic_worth.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- Adding parent_tree_number to seedlot_parent_tree table. Need to recreate the tables that references seedlot_parent_tree (seedlot_parent_tree_smp_mix and seedlot_parent_tree_gen_qlty).
drop table spar.seedlot_parent_tree_smp_mix;
drop table spar.seedlot_parent_tree_gen_qlty;
drop table spar.seedlot_parent_tree;

create table spar.seedlot_parent_tree (
  seedlot_number                varchar(5) not null,
  parent_tree_id                int not null,
  parent_tree_number            varchar(5) not null,
  cone_count                    decimal(20, 10) not null,
  pollen_count                  decimal(20, 10) not null,
  smp_success_pct               int,
  non_orchard_pollen_contam_pct int,
  total_genetic_worth_contrib   decimal(15, 11),
  entry_userid                  varchar(30) not null,
  entry_timestamp               timestamp not null,
  update_userid                 varchar(30) not null,
  update_timestamp              timestamp not null,
  revision_count                int not null,
  constraint seedlot_parent_tree_pk
    primary key(seedlot_number, parent_tree_id),
  constraint seedlot_parent_tree_seedlot_fk
    foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);

comment on table spar.seedlot_parent_tree is 'The contribution of Parent Trees (with their cone and pollen quantity of each Parent Tree) to an Orchard Seedlot (Genetic Class = "A")';
comment on column spar.seedlot_parent_tree.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_parent_tree.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.seedlot_parent_tree.parent_tree_number is 'The original registration number given to a Parent Tree in conjunction with a Species Code.';
comment on column spar.seedlot_parent_tree.cone_count is 'The number of cones counted or estimated from each Parent Tree for a Seedlot.';
comment on column spar.seedlot_parent_tree.pollen_count is 'The amount of pollen counted or estimated for each Parent Tree in the Seedlot.';
comment on column spar.seedlot_parent_tree.smp_success_pct is 'The estimated success (percent) of the supplemental mass pollination mix on the Parent Trees in the Orchard.';
comment on column spar.seedlot_parent_tree.non_orchard_pollen_contam_pct is 'Non-orchard pollen contamination (%).';
comment on column spar.seedlot_parent_tree.total_genetic_worth_contrib is 'Total Parent (seed) genetic worth contribution to seedlot';
comment on column spar.seedlot_parent_tree.entry_userid is 'The userid of the individual that entered the Seedlot Parent Tree.';
comment on column spar.seedlot_parent_tree.entry_timestamp is 'The time and date a Seedlot Parent Tree was entered onto the system.';
comment on column spar.seedlot_parent_tree.update_userid is 'The userid of the individual that changed the Seedlot Parent Tree.';
comment on column spar.seedlot_parent_tree.update_timestamp is 'The time and date a Seedlot Parent Tree was last updated on the system.';
comment on column spar.seedlot_parent_tree.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';


create table spar.seedlot_parent_tree_gen_qlty (
  seedlot_number        varchar(5) not null,
  parent_tree_id        int not null,
  genetic_type_code     varchar(2) not null,
  genetic_worth_code    varchar(3) not null,
  genetic_quality_value decimal(4, 1) not null,
  estimated_ind         boolean,
  untested_ind          boolean,
  entry_userid          varchar(30) not null,
  entry_timestamp       timestamp not null,
  update_userid         varchar(30) not null,
  update_timestamp      timestamp not null,
  revision_count        int,
  constraint seedlot_parent_tree_gen_qlt_pk
    primary key(seedlot_number, parent_tree_id, genetic_type_code, genetic_worth_code),
  constraint sl_ptree_genqly_sl_ptree_fk
    foreign key(seedlot_number, parent_tree_id) references spar.seedlot_parent_tree(seedlot_number, parent_tree_id)
);

comment on table spar.seedlot_parent_tree_gen_qlty is 'The Genetic Worth value(s) for an Orchard Seedlot (Genetic Class = "A") calculated from the Genetic Quality (Breeding Values) of the Parent Trees contributing to the Seedlot.';
comment on column spar.seedlot_parent_tree_gen_qlty.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_parent_tree_gen_qlty.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.seedlot_parent_tree_gen_qlty.genetic_type_code is 'Describes the comparative measure of genetic value for a specific genetic trait of a Parent Tree.  Examples are BV (Breeding Value) and CV (Clonal Value).';
comment on column spar.seedlot_parent_tree_gen_qlty.genetic_worth_code is 'A code describing various Genetic Worths.';
comment on column spar.seedlot_parent_tree_gen_qlty.genetic_quality_value is 'The Genetic Quality value based on the test assessment for a Parent Tree from a test no. and series.';
comment on column spar.seedlot_parent_tree_gen_qlty.estimated_ind is 'Indicates whether or not the test result was estimated. A Genetic Quality value of 2.0 is estimated for Untested Parent Trees (i.e. Parent Trees without an Area of Use) or for Tested Parent Trees (i.e. Parent Trees with an Area of Use) without a Genetic.';
comment on column spar.seedlot_parent_tree_gen_qlty.untested_ind is 'Indicates whether or not the test result was estimated for an untested Parent Tree.';
comment on column spar.seedlot_parent_tree_gen_qlty.entry_userid is 'The userid of the individual that entered the Seedlot Parent Tree Genetic Quality.';
comment on column spar.seedlot_parent_tree_gen_qlty.entry_timestamp is 'The time and date a Seedlot Parent Tree Genetic Quality was entered onto the system.';
comment on column spar.seedlot_parent_tree_gen_qlty.update_userid is 'The userid of the individual that changed the Seedlot Parent Tree Genetic Quality.';
comment on column spar.seedlot_parent_tree_gen_qlty.update_timestamp is 'The time and date a Seedlot Parent Tree Genetic Quality was last updated on the system.';
comment on column spar.seedlot_parent_tree_gen_qlty.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

create table spar.seedlot_parent_tree_smp_mix (
  seedlot_number        varchar(5) not null,
  parent_tree_id        int not null,
  genetic_type_code     varchar(2) not null,
  genetic_worth_code    varchar(3) not null,
  genetic_quality_value decimal(4, 1) not null,
  entry_userid          varchar(30) not null,
  entry_timestamp       timestamp not null,
  update_userid         varchar(30) not null,
  update_timestamp      timestamp not null,
  revision_count        int not null,
  constraint seedlot_parent_tree_smp_mix_pk
    primary key(seedlot_number, parent_tree_id, genetic_type_code, genetic_worth_code),
  constraint sl_ptree_smp_mix_sl_ptree_fk
    foreign key(seedlot_number, parent_tree_id) references spar.seedlot_parent_tree(seedlot_number, parent_tree_id)
);

comment on table spar.seedlot_parent_tree_smp_mix is 'Supplemental Mass Polination for a given Seedlot, Parent Tree, Genetic Type and Genetic worth combination.';
comment on column spar.seedlot_parent_tree_smp_mix.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_parent_tree_smp_mix.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.seedlot_parent_tree_smp_mix.genetic_type_code is 'Describes the comparative measure of genetic value for a specific genetic trait of a Parent Tree. Examples are BV (Breeding Value) and CV (Clonal Value).';;
comment on column spar.seedlot_parent_tree_smp_mix.genetic_worth_code is 'A code describing various Genetic Worths.';;
comment on column spar.seedlot_parent_tree_smp_mix.genetic_quality_value is 'The Genetic Quality value based on the test assessment for a Parent Tree from a test no. and series.';
comment on column spar.seedlot_parent_tree_smp_mix.entry_userid is 'The userid of the individual that entered the Parent Tree SMP mix';
comment on column spar.seedlot_parent_tree_smp_mix.entry_timestamp is 'The time and date a Parent Tree SMP mix was entered onto the system.';
comment on column spar.seedlot_parent_tree_smp_mix.update_userid is 'The userid of the individual that changed the Parent Tree SMP mix.';
comment on column spar.seedlot_parent_tree_smp_mix.update_timestamp is 'The time and date a Parent Tree SMP mix was last updated on the system.';
comment on column spar.seedlot_parent_tree_smp_mix.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

-- Adding parent_tree_number to seedlot_parent_tree table. Need to recreate the table that references smp_mix (smp_mix_gen_qlty).
drop table spar.smp_mix_gen_qlty;
drop table spar.smp_mix;

create table spar.smp_mix (
  seedlot_number     varchar(5) not null,
  parent_tree_id     int not null,
  parent_tree_number varchar(5) not null,
  amount_of_material int not null,
  proportion         decimal(20,10),
  entry_userid       varchar(30) not null,
  entry_timestamp    timestamp not null,
  update_userid      varchar(30) not null,
  update_timestamp   timestamp not null,
  revision_count     int not null,
  constraint smp_mix_pk
    primary key(seedlot_number, parent_tree_id),
  constraint smp_mix_seedlot_fk
    foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);

comment on table spar.smp_mix is 'A table listing the individual Parent Trees that contributed to the Supplemental Mass Pollination mix of an Orchard Seedlot (Genetic Class = "A").';
comment on column spar.smp_mix.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.smp_mix.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.smp_mix.parent_tree_number is 'The original registration number given to a Parent Tree in conjunction with a Species Code.';
comment on column spar.smp_mix.amount_of_material is 'The amount of Parent Tree material used for calculating the proportion of mix. Usually a volume recorded in mL.';
comment on column spar.smp_mix.proportion is 'The proportion of each Parent Tree material used for calculating SMP mix.';
comment on column spar.smp_mix.entry_userid is 'The userid of the individual that entered the SMP mix.';
comment on column spar.smp_mix.entry_timestamp is 'The time and date a SMP mix was entered onto the system.';
comment on column spar.smp_mix.update_userid is 'The userid of the individual that changed the SMP mix.';
comment on column spar.smp_mix.update_timestamp is 'The time and date a SMP mix was last updated on the system.';
comment on column spar.smp_mix.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';


create table spar.smp_mix_gen_qlty (
  seedlot_number        varchar(5) not null,
  parent_tree_id        int not null,
  genetic_type_code     varchar(2) not null,
  genetic_worth_code    varchar(3) not null,
  genetic_quality_value decimal(4, 1) not null,
  estimated_ind         boolean,
  entry_userid          varchar(30) not null,
  entry_timestamp       timestamp not null,
  update_userid         varchar(30) not null,
  update_timestamp      timestamp not null,
  revision_count        int not null,
  constraint smp_mix_gen_qlty_pk
    primary key(seedlot_number, parent_tree_id, genetic_type_code, genetic_worth_code),
  constraint smp_mix_gen_qlty_smp_mix_pk
    foreign key(seedlot_number, parent_tree_id) references spar.smp_mix(seedlot_number, parent_tree_id)
);

comment on table spar.smp_mix_gen_qlty is 'The calculated Genetic Worth value(s) for the Supplemental Mass Pollination mix that contributed to an Orchard Seedlot (Genetic Class = "A") the Parent Trees contributing to a Seedlot.';
comment on column spar.smp_mix_gen_qlty.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.smp_mix_gen_qlty.parent_tree_id is 'A unique identifier for each Parent Tree.';
comment on column spar.smp_mix_gen_qlty.genetic_type_code is 'Describes the comparative measure of genetic value for a specific genetic trait of a Parent Tree. Examples are BV (Breeding Value) and CV (Clonal Value).';
comment on column spar.smp_mix_gen_qlty.genetic_worth_code is 'A code describing various Genetic Worths.';
comment on column spar.smp_mix_gen_qlty.genetic_quality_value is 'The Genetic Quality value based on the test assessment for a Parent Tree from a test no. and series.';
comment on column spar.smp_mix_gen_qlty.entry_userid is 'The userid of the individual that entered the SMP mix Genetic Quality.';
comment on column spar.smp_mix_gen_qlty.entry_timestamp is 'The time and date a SMP mix Genetic Quality was entered onto the system.';
comment on column spar.smp_mix_gen_qlty.update_userid is 'The userid of the individual that changed the SMP mix Genetic Quality.';
comment on column spar.smp_mix_gen_qlty.update_timestamp is 'The time and date a SMP mix Genetic Quality was last updated on the system.';
comment on column spar.smp_mix_gen_qlty.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';

create table spar.seedlot_smp_mix (
  seedlot_number        varchar(5) not null,
  genetic_worth_code    varchar(3) not null,
  genetic_quality_value decimal(4, 1) not null,
  entry_userid          varchar(30) not null,
  entry_timestamp       timestamp not null,
  update_userid         varchar(30) not null,
  update_timestamp      timestamp not null,
  revision_count        int not null,
  constraint seedlot_smp_mix_pk
    primary key(seedlot_number, genetic_worth_code),
  constraint seedlot_smp_mix_seedlot_fk
    foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);-- Geographic data columns
alter table spar.seedlot
  add column seed_plan_unit_id         smallint,
  add column bgc_zone_code             varchar(4),
  add column bgc_subzone_code          varchar(3),
  add column variant                   varchar(1),
  add column bec_version_id            smallint,
  add column elevation                 smallint,
  add column latitude_degrees          smallint,
  add column latitude_minutes          smallint,
  add column latitude_seconds          smallint,
  add column longitude_degrees         smallint,
  add column longitude_minutes         smallint,
  add column longitude_seconds         smallint,
  add column collection_elevation      smallint,
  add column collection_elevation_min  smallint,
  add column collection_elevation_max  smallint,
  add column collection_latitude_deg   smallint,
  add column collection_latitude_min   smallint,
  add column collection_latitude_sec   smallint,
  add column collection_latitude_code  varchar(1),
  add column collection_longitude_deg  smallint,
  add column collection_longitude_min  smallint,
  add column collection_longitude_sec  smallint,
  add column collection_longitude_code varchar(1),
  add column elevation_min             smallint,
  add column elevation_max             smallint,
  add column latitude_deg_min          smallint,
  add column latitude_min_min          smallint,
  add column latitude_sec_min          smallint,
  add column latitude_deg_max          smallint,
  add column latitude_min_max          smallint,
  add column latitude_sec_max          smallint,
  add column longitude_deg_min         smallint,
  add column longitude_min_min         smallint,
  add column longitude_sec_min         smallint,
  add column longitude_deg_max         smallint,
  add column longitude_min_max         smallint,
  add column longitude_sec_max         smallint,
  add column smp_mean_bv_growth        decimal(4, 1),
  add column area_of_use_comment       varchar(2000);

comment on column spar.seedlot.seed_plan_unit_id is 'A unique identifier which is assigned to a Seed Planning Unit.';
comment on column spar.seedlot.bgc_zone_code is 'The Biogeoclimatic Zone of the Seedlot collection area.';
comment on column spar.seedlot.bgc_subzone_code is 'The Biogeoclimatic Subzone of the Seedlot collection area.';
comment on column spar.seedlot.variant is 'A division of biogeoclimatic information, with multiple variants in a BGC Subzone.';
comment on column spar.seedlot.bec_version_id is 'A sequence generated identifier for each BEC version.';
comment on column spar.seedlot.elevation is 'The representative elevation in meters where the seed lot originated, which is also the mean area of use elevation for natural stand lots. For orchard seed lots it is a weighted average of the contributing parent trees source elevations.';
comment on column spar.seedlot.latitude_degrees is 'The representative latitude (degrees) where the seed lot originated, which is also the mean area of use latitude for natural stand lots. For orchard seed lots it is a weighted average of the contributing parent trees source latitudes.';
comment on column spar.seedlot.latitude_minutes is 'The representative latitude (minutes) where the seed lot originated, which is also the mean area of use latitude for natural stand lots. For orchard seed lots it is a weighted average of the contributing parent trees source latitudes.';
comment on column spar.seedlot.latitude_seconds is 'The representative latitude (seconds) where the seed lot originated, which is also the mean area of use latitude for natural stand lots. For orchard seed lots it is a weighted average of the contributing parent trees source latitudes.';
comment on column spar.seedlot.longitude_degrees is 'The representative longitude (degrees) where the seed lot originated, which is also the mean area of use longitude for natural stand lots. For orchard seed lots it is a weighted average of the contributing parent trees source longitudes.';
comment on column spar.seedlot.longitude_minutes is 'The representative longitude (minutes) where the seed lot originated, which is also the mean area of use longitude for natural stand lots. For orchard seed lots it is a weighted average of the contributing parent trees source longitudes.';
comment on column spar.seedlot.longitude_seconds is 'The representative longitude (seconds) where the seed lot originated, which is also the mean area of use longitude for natural stand lots. For orchard seed lots it is a weighted average of the contributing parent trees source longitudes.';
comment on column spar.seedlot.collection_elevation is 'The representative elevation in meters of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source elevations.';
comment on column spar.seedlot.collection_elevation_min is 'The representative minimum elevation in meters of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source elevations.';
comment on column spar.seedlot.collection_elevation_max is 'The representative maximum elevation in meters of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source elevations.';
comment on column spar.seedlot.collection_latitude_deg is 'The representative latitude (degrees) of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source latitude.';
comment on column spar.seedlot.collection_latitude_min is 'The representative latitude (minutes) of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source latitude.';
comment on column spar.seedlot.collection_latitude_sec is 'The representative latitude (seconds) of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source latitude.';
comment on column spar.seedlot.collection_latitude_code is 'A code which specifies whether the exotic origin latitude is north (N) or south (S).';
comment on column spar.seedlot.collection_longitude_deg is 'The representative longitude (degrees) of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source longitude.';
comment on column spar.seedlot.collection_longitude_min is 'The representative longitude (minutes) of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source longitude.';
comment on column spar.seedlot.collection_longitude_sec is 'The representative longitude (seconds) of the Seedlot collection area. For orchard seedlots it is a weighted average of the contributing parent trees source longitude.';
comment on column spar.seedlot.collection_longitude_code is 'A code which specifies whether the exotic origin longitude is east (E) or west (W).';
comment on column spar.seedlot.elevation_min is 'The minimum elevation of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.elevation_max is 'The maximum elevation of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.latitude_deg_min is 'The minimum latitude (degrees) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.latitude_min_min is 'The minimum latitude (minutes) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.latitude_sec_min is 'The minimum latitude (seconds) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.latitude_deg_max is 'The maximum latitude (degrees) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.latitude_min_max is 'The maximum latitude (minutes) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.latitude_sec_max is 'The maximum latitude (seconds) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.longitude_deg_min is 'The minimum longitude (degrees) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.longitude_min_min is 'The minimum longitude (minutes) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.longitude_sec_min is 'The minimum longitude (seconds) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.longitude_deg_max is 'The maximum longitude (degrees) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.longitude_min_max is 'The maximum longitude (minutes) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.longitude_sec_max is 'The maximum longitude (seconds) of the seedlot area of use. For lots from natural stands, this is calculated using the transfer limits. For lots from tested parent trees (orchards) either use Seed Planning Unit minimum or the collection latitude.';
comment on column spar.seedlot.smp_mean_bv_growth is 'The breeding value for the Growth trait calculated from the supplemental mass pollination mix applied to parent trees.';
comment on column spar.seedlot.area_of_use_comment is 'Comments on Seedlot area of use.';

create table spar.seedlot_seed_plan_zone (
  seedlot_number      varchar(5) not null,
  seed_plan_zone_code varchar(3) not null,
  entry_userid        varchar(30) not null,
  entry_timestamp     timestamp not null,
  update_userid       varchar(30) not null,
  update_timestamp    timestamp not null,
  revision_count      int not null,
  constraint seedlot_seed_plan_zone_pk
    primary key(seedlot_number, seed_plan_zone_code),
  constraint seedlot_seedplan_zn_seedlot_fk
    foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);

comment on column spar.seedlot_seed_plan_zone.seedlot_number is 'The unique number (key) assigned to a quantity of seed of a particular species and quality from a given location collected at a given time.';
comment on column spar.seedlot_seed_plan_zone.seed_plan_zone_code is 'A code describing various Seed Planning Zones.';
comment on column spar.seedlot_seed_plan_zone.entry_userid is 'The userid of the individual that entered the Seedlot collection method.';
comment on column spar.seedlot_seed_plan_zone.entry_timestamp is 'The time and date a Seedlot collection method was entered onto the system.';
comment on column spar.seedlot_seed_plan_zone.update_userid is 'The userid of the individual that changed the Seedlot collection method.';
comment on column spar.seedlot_seed_plan_zone.update_timestamp is 'The time and date a Seedlot collection method was last updated on the system.';
comment on column spar.seedlot_seed_plan_zone.revision_count is 'A counter used to ensure data integrity. This item should be incremented during each update.';
/**
 * seedlot_genetic_worth
 */
alter table
  spar.seedlot_genetic_worth
alter column
  entry_timestamp
set
  default current_timestamp;

alter table
  spar.seedlot_genetic_worth
alter column
  update_timestamp
set
  default current_timestamp;

/**
 * seedlot_parent_tree
 */
alter table
  spar.seedlot_parent_tree
alter column
  entry_timestamp
set
  default current_timestamp;

alter table
  spar.seedlot_parent_tree
alter column
  update_timestamp
set
  default current_timestamp;

/**
 * seedlot_parent_tree_gen_qlty
 */
alter table
  spar.seedlot_parent_tree_gen_qlty
alter column
  entry_timestamp
set
  default current_timestamp;

alter table
  spar.seedlot_parent_tree_gen_qlty
alter column
  update_timestamp
set
  default current_timestamp;

/**
 * seedlot_parent_tree_smp_mix
 */
alter table
  spar.seedlot_parent_tree_smp_mix
alter column
  entry_timestamp
set
  default current_timestamp;

alter table
  spar.seedlot_parent_tree_smp_mix
alter column
  update_timestamp
set
  default current_timestamp;

/**
 * smp_mix
 */
alter table
  spar.smp_mix
alter column
  entry_timestamp
set
  default current_timestamp;

alter table
  spar.smp_mix
alter column
  update_timestamp
set
  default current_timestamp;

/**
 * smp_mix_gen_qlty
 */
alter table
  spar.smp_mix_gen_qlty
alter column
  entry_timestamp
set
  default current_timestamp;

alter table
  spar.smp_mix_gen_qlty
alter column
  update_timestamp
set
  default current_timestamp;

/**
 * seedlot_smp_mix
 */
alter table
  spar.seedlot_smp_mix
alter column
  entry_timestamp
set
  default current_timestamp;

alter table
  spar.seedlot_smp_mix
alter column
  update_timestamp
set
  default current_timestamp;

/**
 * seedlot_seed_plan_zone
 */
alter table
  spar.seedlot_seed_plan_zone
alter column
  entry_timestamp
set
  default current_timestamp;

alter table
  spar.seedlot_seed_plan_zone
alter column
  update_timestamp
set
  default current_timestamp;

create table spar.seedlot_registration_a_class_save (
  seedlot_number varchar(5) not null,
  all_step_data jsonb not null,
  progress_status jsonb not null,
  entry_userid varchar(30) not null,
  entry_timestamp timestamp default current_timestamp,
  update_userid varchar(30) not null,
  update_timestamp timestamp default current_timestamp,
  revision_count int not null,
  constraint registration_form_a_class_pk primary key(seedlot_number),
  constraint registration_form_a_class_seedlot_fk foreign key(seedlot_number) references spar.seedlot(seedlot_number)
);

alter table
  spar.seedlot_seed_plan_zone
add
  seed_plan_zone_id smallint,
add
  genetic_class_code varchar(1),
add
  constraint fk_genetic_class_code foreign key (genetic_class_code) references spar.genetic_class_list(genetic_class_code);

comment on column spar.seedlot_seed_plan_zone.seed_plan_zone_id is 'A unique identifier for a combination of Seed Plan Zone Code, Genetic Class and Vegetation Code';

comment on column spar.seedlot_seed_plan_zone.genetic_class_code is 'A code describing various Genetic Classes.';
create table if not exists spar.seedlot_audit (
  seedlot_audit_id                serial      not null,
  audit_date                      timestamp   default now() not null,
  spar_audit_code                 varchar(1)  default 'I' not null,
  db_user						  text        not null,
  audit_revision_version		  int         not null default 1,
  seedlot_number                  varchar(5)  not null,
  seedlot_status_code             varchar(3)  not null,
  applicant_client_number         varchar(8),
  applicant_locn_code             varchar(2),
  applicant_email_address         varchar(100),
  vegetation_code                 varchar(8),
  genetic_class_code              varchar(1),
  seedlot_source_code             varchar(3),
  to_be_registrd_ind              boolean,
  bc_source_ind                   boolean,
  collection_client_number        varchar(8),
  collection_locn_code            varchar(2),
  collection_start_date           timestamp,
  collection_end_date             timestamp,
  no_of_containers                decimal(6, 2),
  vol_per_container               decimal(6, 2),
  clctn_volume                    decimal(6, 2),
  seedlot_comment                 varchar(2000),
  interm_strg_client_number       varchar(8),
  interm_strg_locn_code           varchar(2),
  interm_strg_st_date             timestamp,
  interm_strg_end_date            timestamp,
  interm_facility_code            varchar(3),
  female_gametic_mthd_code        varchar(4),
  male_gametic_mthd_code          varchar(4),
  controlled_cross_ind            boolean,
  biotech_processes_ind           boolean,
  pollen_contamination_ind        boolean,
  pollen_contamination_pct        int,
  contaminant_pollen_bv           decimal(4, 1),
  pollen_contamination_mthd_code  varchar(4),
  total_parent_trees              int,
  smp_success_pct                 int,
  effective_pop_size              decimal(5, 1),
  smp_parents_outside             int,
  non_orchard_pollen_contam_pct   int,
  extractory_client_number        varchar(8),
  extractory_locn_code            varchar(2),
  extraction_st_date              timestamp,
  extraction_end_date             timestamp,
  temporary_strg_client_number    varchar(8),
  temporary_strg_locn_code        varchar(2),
  temporary_strg_start_date       timestamp,
  temporary_strg_end_date         timestamp,
  interm_strg_locn                varchar(55),
  declared_userid                 varchar(30),
  declared_timestamp              timestamp,
  entry_userid                    varchar(30) not null,
  entry_timestamp                 timestamp not null,
  update_userid                   varchar(30) not null,
  update_timestamp                timestamp not null,
  revision_count                  int not null,
  constraint seedlot_audit_pk
    primary key(seedlot_audit_id)
);


comment on column spar.seedlot_audit.seedlot_audit_id               is 'Serial Incremental number referring to a unique primary key for this table';
comment on column spar.seedlot_audit.audit_date                     is 'Date when this audit record was created (representing with some precision when the statement was executed)';
comment on column spar.seedlot_audit.spar_audit_code                is 'Type of statement executed in spar.seedlot table: I=INSERT, U=UPDATE, D=DELETE. Note that DELETE record represents only the latest data whiped out from the seedlot table';
comment on column spar.seedlot_audit.db_user                 		is 'Postgres database user who executed the statement in spar.seedlot table';
comment on column spar.seedlot_audit.audit_revision_version         is 'Record registered version for audit purposes.';
comment on column spar.seedlot_audit.seedlot_number                 is 'Referring value for spar.seedlot.seedlot_number column';
comment on column spar.seedlot_audit.seedlot_status_code            is 'Referring value for spar.seedlot.seedlot_status_code column';
comment on column spar.seedlot_audit.applicant_client_number        is 'Referring value for spar.seedlot.applicant_client_number column';
comment on column spar.seedlot_audit.applicant_locn_code            is 'Referring value for spar.seedlot.applicant_locn_code column';
comment on column spar.seedlot_audit.applicant_email_address        is 'Referring value for spar.seedlot.applicant_email_address column';
comment on column spar.seedlot_audit.vegetation_code                is 'Referring value for spar.seedlot.vegetation_code column';
comment on column spar.seedlot_audit.genetic_class_code             is 'Referring value for spar.seedlot.genetic_class_code column';
comment on column spar.seedlot_audit.seedlot_source_code            is 'Referring value for spar.seedlot.seedlot_source_code column';
comment on column spar.seedlot_audit.to_be_registrd_ind             is 'Referring value for spar.seedlot.to_be_registrd_ind column';
comment on column spar.seedlot_audit.bc_source_ind                  is 'Referring value for spar.seedlot.bc_source_ind column';
comment on column spar.seedlot_audit.collection_client_number       is 'Referring value for spar.seedlot.collection_client_number column';
comment on column spar.seedlot_audit.collection_locn_code           is 'Referring value for spar.seedlot.collection_locn_code column';
comment on column spar.seedlot_audit.collection_start_date          is 'Referring value for spar.seedlot.collection_start_date column';
comment on column spar.seedlot_audit.collection_end_date            is 'Referring value for spar.seedlot.collection_end_date column';
comment on column spar.seedlot_audit.no_of_containers               is 'Referring value for spar.seedlot.no_of_containers column';
comment on column spar.seedlot_audit.vol_per_container              is 'Referring value for spar.seedlot.vol_per_container column';
comment on column spar.seedlot_audit.clctn_volume                   is 'Referring value for spar.seedlot.clctn_volume column';
comment on column spar.seedlot_audit.seedlot_comment                is 'Referring value for spar.seedlot.seedlot_comment column';
comment on column spar.seedlot_audit.interm_strg_client_number      is 'Referring value for spar.seedlot.interm_strg_client_number column';
comment on column spar.seedlot_audit.interm_strg_locn_code          is 'Referring value for spar.seedlot.interm_strg_locn_code column';
comment on column spar.seedlot_audit.interm_strg_st_date            is 'Referring value for spar.seedlot.interm_strg_st_date column';
comment on column spar.seedlot_audit.interm_strg_end_date           is 'Referring value for spar.seedlot.interm_strg_end_date column';
comment on column spar.seedlot_audit.interm_facility_code           is 'Referring value for spar.seedlot.interm_facility_code column';
comment on column spar.seedlot_audit.female_gametic_mthd_code       is 'Referring value for spar.seedlot.female_gametic_mthd_code column';
comment on column spar.seedlot_audit.male_gametic_mthd_code         is 'Referring value for spar.seedlot.male_gametic_mthd_code column';
comment on column spar.seedlot_audit.controlled_cross_ind           is 'Referring value for spar.seedlot.controlled_cross_ind column';
comment on column spar.seedlot_audit.biotech_processes_ind          is 'Referring value for spar.seedlot.biotech_processes_ind column';
comment on column spar.seedlot_audit.pollen_contamination_ind       is 'Referring value for spar.seedlot.pollen_contamination_ind column';
comment on column spar.seedlot_audit.pollen_contamination_pct       is 'Referring value for spar.seedlot.pollen_contamination_pct column';
comment on column spar.seedlot_audit.contaminant_pollen_bv          is 'Referring value for spar.seedlot.contaminant_pollen_bv column';
comment on column spar.seedlot_audit.pollen_contamination_mthd_code is 'Referring value for spar.seedlot.pollen_contamination_mthd_code column';
comment on column spar.seedlot_audit.total_parent_trees             is 'Referring value for spar.seedlot.total_parent_trees column';
comment on column spar.seedlot_audit.smp_success_pct                is 'Referring value for spar.seedlot.smp_success_pct column';
comment on column spar.seedlot_audit.effective_pop_size             is 'Referring value for spar.seedlot.effective_pop_size column';
comment on column spar.seedlot_audit.smp_parents_outside            is 'Referring value for spar.seedlot.smp_parents_outside column';
comment on column spar.seedlot_audit.non_orchard_pollen_contam_pct  is 'Referring value for spar.seedlot.non_orchard_pollen_contam_pct column';
comment on column spar.seedlot_audit.extractory_client_number       is 'Referring value for spar.seedlot.extractory_client_number column';
comment on column spar.seedlot_audit.extractory_locn_code           is 'Referring value for spar.seedlot.extractory_locn_code column';
comment on column spar.seedlot_audit.extraction_st_date             is 'Referring value for spar.seedlot.extraction_st_date column';
comment on column spar.seedlot_audit.extraction_end_date            is 'Referring value for spar.seedlot.extraction_end_date column';
comment on column spar.seedlot_audit.temporary_strg_client_number   is 'Referring value for spar.seedlot.temporary_strg_client_number column';
comment on column spar.seedlot_audit.temporary_strg_locn_code       is 'Referring value for spar.seedlot.temporary_strg_locn_code column';
comment on column spar.seedlot_audit.temporary_strg_start_date      is 'Referring value for spar.seedlot.temporary_strg_start_date column';
comment on column spar.seedlot_audit.temporary_strg_end_date        is 'Referring value for spar.seedlot.temporary_strg_end_date column';
comment on column spar.seedlot_audit.declared_userid                is 'Referring value for spar.seedlot.declared_userid column';
comment on column spar.seedlot_audit.declared_timestamp             is 'Referring value for spar.seedlot.declared_timestamp column';
comment on column spar.seedlot_audit.entry_userid                   is 'Referring value for spar.seedlot.entry_userid column';
comment on column spar.seedlot_audit.entry_timestamp                is 'Referring value for spar.seedlot.entry_timestamp column';
comment on column spar.seedlot_audit.update_userid                  is 'Referring value for spar.seedlot.update_userid column';
comment on column spar.seedlot_audit.update_timestamp               is 'Referring value for spar.seedlot.update_timestamp column';
comment on column spar.seedlot_audit.revision_count                 is 'Referring value for spar.seedlot.revision_count column';/*
-- Function to catch Insert/Update/Delete staments executed in seedlot table.
-- Should be used in a trigger in seedlot table
-- trigger statement in the bottom part of this script
*/
CREATE OR REPLACE FUNCTION spar.seedlot_if_modified_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
	v_auditrevision int;
BEGIN


    if (TG_OP = 'UPDATE') then
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
		/* AUDIT REVISION number used to order the statements executed in the row */
		v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number) ;
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
		VALUES(
		/*spar_audit_code                */ 'U',
		/*db_user                 	     */ session_user::TEXT,
		/*audit_revision_version  	     */ coalesce(v_auditrevision,1),
		/*seedlot_number                 */ NEW.seedlot_number,
		/*seedlot_status_code            */ NEW.seedlot_status_code,
		/*applicant_client_number        */ NEW.applicant_client_number,
		/*applicant_locn_code            */ NEW.applicant_locn_code,
		/*applicant_email_address        */ NEW.applicant_email_address,
		/*vegetation_code                */ NEW.vegetation_code,
		/*genetic_class_code             */ NEW.genetic_class_code,
		/*seedlot_source_code            */ NEW.seedlot_source_code,
		/*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
		/*bc_source_ind                  */ NEW.bc_source_ind,
		/*collection_client_number       */ NEW.collection_client_number,
		/*collection_locn_code           */ NEW.collection_locn_code,
		/*collection_start_date          */ NEW.collection_start_date,
		/*collection_end_date            */ NEW.collection_end_date,
		/*no_of_containers               */ NEW.no_of_containers,
		/*vol_per_container              */ NEW.vol_per_container,
		/*clctn_volume                   */ NEW.clctn_volume,
		/*seedlot_comment                */ NEW.seedlot_comment,
		/*interm_strg_client_number      */ NEW.interm_strg_client_number,
		/*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
		/*interm_strg_st_date            */ NEW.interm_strg_st_date,
		/*interm_strg_end_date           */ NEW.interm_strg_end_date,
		/*interm_facility_code           */ NEW.interm_facility_code,
		/*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
		/*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
		/*controlled_cross_ind           */ NEW.controlled_cross_ind,
		/*biotech_processes_ind          */ NEW.biotech_processes_ind,
		/*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
		/*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
		/*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
		/*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
		/*total_parent_trees             */ NEW.total_parent_trees,
		/*smp_success_pct                */ NEW.smp_success_pct,
		/*effective_pop_size             */ NEW.effective_pop_size,
		/*smp_parents_outside            */ NEW.smp_parents_outside,
		/*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
		/*extractory_client_number       */ NEW.extractory_client_number,
		/*extractory_locn_code           */ NEW.extractory_locn_code,
		/*extraction_st_date             */ NEW.extraction_st_date,
		/*extraction_end_date            */ NEW.extraction_end_date,
		/*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
		/*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
		/*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
		/*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
		/*declared_userid                */ NEW.declared_userid,
		/*declared_timestamp             */ NEW.declared_timestamp,
		/*entry_userid                   */ NEW.entry_userid,
		/*entry_timestamp                */ NEW.entry_timestamp,
		/*update_userid                  */ NEW.update_userid,
		/*update_timestamp               */ NEW.update_timestamp,
		/*revision_count                 */ NEW.revision_count
		);
        RETURN NEW;
    elsif (TG_OP = 'DELETE') then
        v_old_data := ROW(OLD.*);
        /*nsert into spar.logged_actions (schema_name,table_name,user_name,action,original_data,query)
        values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data, current_query());*/
		/* AUDIT REVISION number used to order the statements executed in the row */
		v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number) ;
		INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
		VALUES(
		/*spar_audit_code                */ 'D',
		/*db_user                 	     */ session_user::TEXT,
		/*audit_revision_version  	     */ coalesce(v_auditrevision,1),
		/*seedlot_number                 */ OLD.seedlot_number,
		/*seedlot_status_code            */ OLD.seedlot_status_code,
		/*applicant_client_number        */ OLD.applicant_client_number,
		/*applicant_locn_code            */ OLD.applicant_locn_code,
		/*applicant_email_address        */ OLD.applicant_email_address,
		/*vegetation_code                */ OLD.vegetation_code,
		/*genetic_class_code             */ OLD.genetic_class_code,
		/*seedlot_source_code            */ OLD.seedlot_source_code,
		/*to_be_registrd_ind             */ OLD.to_be_registrd_ind,
		/*bc_source_ind                  */ OLD.bc_source_ind,
		/*collection_client_number       */ OLD.collection_client_number,
		/*collection_locn_code           */ OLD.collection_locn_code,
		/*collection_start_date          */ OLD.collection_start_date,
		/*collection_end_date            */ OLD.collection_end_date,
		/*no_of_containers               */ OLD.no_of_containers,
		/*vol_per_container              */ OLD.vol_per_container,
		/*clctn_volume                   */ OLD.clctn_volume,
		/*seedlot_comment                */ OLD.seedlot_comment,
		/*interm_strg_client_number      */ OLD.interm_strg_client_number,
		/*interm_strg_locn_code          */ OLD.interm_strg_locn_code,
		/*interm_strg_st_date            */ OLD.interm_strg_st_date,
		/*interm_strg_end_date           */ OLD.interm_strg_end_date,
		/*interm_facility_code           */ OLD.interm_facility_code,
		/*female_gametic_mthd_code       */ OLD.female_gametic_mthd_code,
		/*male_gametic_mthd_code         */ OLD.male_gametic_mthd_code,
		/*controlled_cross_ind           */ OLD.controlled_cross_ind,
		/*biotech_processes_ind          */ OLD.biotech_processes_ind,
		/*pollen_contamination_ind       */ OLD.pollen_contamination_ind,
		/*pollen_contamination_pct       */ OLD.pollen_contamination_pct,
		/*contaminant_pollen_bv          */ OLD.contaminant_pollen_bv,
		/*pollen_contamination_mthd_code */ OLD.pollen_contamination_mthd_code,
		/*total_parent_trees             */ OLD.total_parent_trees,
		/*smp_success_pct                */ OLD.smp_success_pct,
		/*effective_pop_size             */ OLD.effective_pop_size,
		/*smp_parents_outside            */ OLD.smp_parents_outside,
		/*non_orchard_pollen_contam_pct  */ OLD.non_orchard_pollen_contam_pct,
		/*extractory_client_number       */ OLD.extractory_client_number,
		/*extractory_locn_code           */ OLD.extractory_locn_code,
		/*extraction_st_date             */ OLD.extraction_st_date,
		/*extraction_end_date            */ OLD.extraction_end_date,
		/*temporary_strg_client_number   */ OLD.temporary_strg_client_number,
		/*temporary_strg_locn_code       */ OLD.temporary_strg_locn_code,
		/*temporary_strg_start_date      */ OLD.temporary_strg_start_date,
		/*temporary_strg_end_date        */ OLD.temporary_strg_end_date,
    /*interm_strg_locn               */ OLD.interm_strg_locn,
		/*declared_userid                */ OLD.declared_userid,
		/*declared_timestamp             */ OLD.declared_timestamp,
		/*entry_userid                   */ OLD.entry_userid,
		/*entry_timestamp                */ OLD.entry_timestamp,
		/*update_userid                  */ OLD.update_userid,
		/*update_timestamp               */ OLD.update_timestamp,
		/*revision_count                 */ OLD.revision_count
		);
        RETURN OLD;
    elsif (TG_OP = 'INSERT') then
        v_new_data := ROW(NEW.*);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
		VALUES(
		/*spar_audit_code                */ 'I',
		/*db_user                 	     */ session_user::TEXT,
		/*audit_revision_version  	     */ 1, -- 1st row version
		/*seedlot_number                 */ NEW.seedlot_number,
		/*seedlot_status_code            */ NEW.seedlot_status_code,
		/*applicant_client_number        */ NEW.applicant_client_number,
		/*applicant_locn_code            */ NEW.applicant_locn_code,
		/*applicant_email_address        */ NEW.applicant_email_address,
		/*vegetation_code                */ NEW.vegetation_code,
		/*genetic_class_code             */ NEW.genetic_class_code,
		/*seedlot_source_code            */ NEW.seedlot_source_code,
		/*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
		/*bc_source_ind                  */ NEW.bc_source_ind,
		/*collection_client_number       */ NEW.collection_client_number,
		/*collection_locn_code           */ NEW.collection_locn_code,
		/*collection_start_date          */ NEW.collection_start_date,
		/*collection_end_date            */ NEW.collection_end_date,
		/*no_of_containers               */ NEW.no_of_containers,
		/*vol_per_container              */ NEW.vol_per_container,
		/*clctn_volume                   */ NEW.clctn_volume,
		/*seedlot_comment                */ NEW.seedlot_comment,
		/*interm_strg_client_number      */ NEW.interm_strg_client_number,
		/*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
		/*interm_strg_st_date            */ NEW.interm_strg_st_date,
		/*interm_strg_end_date           */ NEW.interm_strg_end_date,
		/*interm_facility_code           */ NEW.interm_facility_code,
		/*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
		/*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
		/*controlled_cross_ind           */ NEW.controlled_cross_ind,
		/*biotech_processes_ind          */ NEW.biotech_processes_ind,
		/*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
		/*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
		/*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
		/*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
		/*total_parent_trees             */ NEW.total_parent_trees,
		/*smp_success_pct                */ NEW.smp_success_pct,
		/*effective_pop_size             */ NEW.effective_pop_size,
		/*smp_parents_outside            */ NEW.smp_parents_outside,
		/*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
		/*extractory_client_number       */ NEW.extractory_client_number,
		/*extractory_locn_code           */ NEW.extractory_locn_code,
		/*extraction_st_date             */ NEW.extraction_st_date,
		/*extraction_end_date            */ NEW.extraction_end_date,
		/*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
		/*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
		/*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
		/*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
		/*declared_userid                */ NEW.declared_userid,
		/*declared_timestamp             */ NEW.declared_timestamp,
		/*entry_userid                   */ NEW.entry_userid,
		/*entry_timestamp                */ NEW.entry_timestamp,
		/*update_userid                  */ NEW.update_userid,
		/*update_timestamp               */ NEW.update_timestamp,
		/*revision_count                 */ NEW.revision_count
		);
        RETURN NEW;
    else
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
        RETURN NULL;
    end if;

EXCEPTION
    WHEN data_exception then
        --insert into spar.error_catch (erro) VALUES(CONCAT('DATA EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN unique_violation then
	    --insert into spar.error_catch (erro) VALUES(CONCAT('UNIQUE EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN others then
        --insert into spar.error_catch (erro) VALUES(CONCAT(v_auditrevision, CONCAT('OTHER EXCEPTION ',SQLERRM)));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, spar;

-- Trigger to be attached on spar.seedlot table
CREATE TRIGGER trg_seedlot_audit_DIU
 AFTER INSERT OR UPDATE OR DELETE ON spar.seedlot
 FOR EACH ROW EXECUTE PROCEDURE spar.seedlot_if_modified_func();
 alter table spar.seedlot_audit
  add column seed_plan_unit_id         smallint,
  add column bgc_zone_code             varchar(4),
  add column bgc_subzone_code          varchar(3),
  add column variant                   varchar(1),
  add column bec_version_id            smallint,
  add column elevation                 smallint,
  add column latitude_degrees          smallint,
  add column latitude_minutes          smallint,
  add column latitude_seconds          smallint,
  add column longitude_degrees         smallint,
  add column longitude_minutes         smallint,
  add column longitude_seconds         smallint,
  add column collection_elevation      smallint,
  add column collection_elevation_min  smallint,
  add column collection_elevation_max  smallint,
  add column collection_latitude_deg   smallint,
  add column collection_latitude_min   smallint,
  add column collection_latitude_sec   smallint,
  add column collection_latitude_code  varchar(1),
  add column collection_longitude_deg  smallint,
  add column collection_longitude_min  smallint,
  add column collection_longitude_sec  smallint,
  add column collection_longitude_code varchar(1),
  add column elevation_min             smallint,
  add column elevation_max             smallint,
  add column latitude_deg_min          smallint,
  add column latitude_min_min          smallint,
  add column latitude_sec_min          smallint,
  add column latitude_deg_max          smallint,
  add column latitude_min_max          smallint,
  add column latitude_sec_max          smallint,
  add column longitude_deg_min         smallint,
  add column longitude_min_min         smallint,
  add column longitude_sec_min         smallint,
  add column longitude_deg_max         smallint,
  add column longitude_min_max         smallint,
  add column longitude_sec_max         smallint,
  add column smp_mean_bv_growth        decimal(4, 1),
  add column area_of_use_comment       varchar(2000);

comment on column spar.seedlot_audit.seed_plan_unit_id          is 'Referring value for spar.seedlot.seed_plan_unit_id column';
comment on column spar.seedlot_audit.bgc_zone_code              is 'Referring value for spar.seedlot.bgc_zone_code column';
comment on column spar.seedlot_audit.bgc_subzone_code           is 'Referring value for spar.seedlot.bgc_subzone_code column';
comment on column spar.seedlot_audit.variant                    is 'Referring value for spar.seedlot.variant column';
comment on column spar.seedlot_audit.bec_version_id             is 'Referring value for spar.seedlot.bec_version_id column';
comment on column spar.seedlot_audit.elevation                  is 'Referring value for spar.seedlot.elevation column';
comment on column spar.seedlot_audit.latitude_degrees           is 'Referring value for spar.seedlot.latitude_degrees column';
comment on column spar.seedlot_audit.latitude_minutes           is 'Referring value for spar.seedlot.latitude_minutes column';
comment on column spar.seedlot_audit.latitude_seconds           is 'Referring value for spar.seedlot.latitude_seconds column';
comment on column spar.seedlot_audit.longitude_degrees          is 'Referring value for spar.seedlot.longitude_degrees column';
comment on column spar.seedlot_audit.longitude_minutes          is 'Referring value for spar.seedlot.longitude_minutes column';
comment on column spar.seedlot_audit.longitude_seconds          is 'Referring value for spar.seedlot.longitude_seconds column';
comment on column spar.seedlot_audit.collection_elevation       is 'Referring value for spar.seedlot.collection_elevation column';
comment on column spar.seedlot_audit.collection_elevation_min   is 'Referring value for spar.seedlot.collection_elevation_min column';
comment on column spar.seedlot_audit.collection_elevation_max   is 'Referring value for spar.seedlot.collection_elevation_max column';
comment on column spar.seedlot_audit.collection_latitude_deg    is 'Referring value for spar.seedlot.collection_latitude_deg column';
comment on column spar.seedlot_audit.collection_latitude_min    is 'Referring value for spar.seedlot.collection_latitude_min column';
comment on column spar.seedlot_audit.collection_latitude_sec    is 'Referring value for spar.seedlot.collection_latitude_sec column';
comment on column spar.seedlot_audit.collection_latitude_code   is 'Referring value for spar.seedlot.collection_latitude_code column';
comment on column spar.seedlot_audit.collection_longitude_deg   is 'Referring value for spar.seedlot.collection_longitude_deg column';
comment on column spar.seedlot_audit.collection_longitude_min   is 'Referring value for spar.seedlot.collection_longitude_min column';
comment on column spar.seedlot_audit.collection_longitude_sec   is 'Referring value for spar.seedlot.collection_longitude_sec column';
comment on column spar.seedlot_audit.collection_longitude_code  is 'Referring value for spar.seedlot.collection_longitude_code column';
comment on column spar.seedlot_audit.elevation_min              is 'Referring value for spar.seedlot.elevation_min column';
comment on column spar.seedlot_audit.elevation_max              is 'Referring value for spar.seedlot.elevation_max column';
comment on column spar.seedlot_audit.latitude_deg_min           is 'Referring value for spar.seedlot.latitude_deg_min column';
comment on column spar.seedlot_audit.latitude_sec_min           is 'Referring value for spar.seedlot.latitude_sec_min column';
comment on column spar.seedlot_audit.latitude_min_min           is 'Referring value for spar.seedlot.latitude_min_min column';
comment on column spar.seedlot_audit.latitude_deg_max           is 'Referring value for spar.seedlot.latitude_deg_max column';
comment on column spar.seedlot_audit.latitude_min_max           is 'Referring value for spar.seedlot.latitude_min_max column';
comment on column spar.seedlot_audit.latitude_sec_max           is 'Referring value for spar.seedlot.latitude_sec_max column';
comment on column spar.seedlot_audit.longitude_deg_min          is 'Referring value for spar.seedlot.longitude_deg_min column';
comment on column spar.seedlot_audit.longitude_min_min          is 'Referring value for spar.seedlot.longitude_min_min column';
comment on column spar.seedlot_audit.longitude_sec_min          is 'Referring value for spar.seedlot.longitude_sec_min column';
comment on column spar.seedlot_audit.longitude_deg_max          is 'Referring value for spar.seedlot.longitude_deg_max column';
comment on column spar.seedlot_audit.longitude_min_max          is 'Referring value for spar.seedlot.longitude_min_max column';
comment on column spar.seedlot_audit.longitude_sec_max          is 'Referring value for spar.seedlot.longitude_sec_max column';
comment on column spar.seedlot_audit.smp_mean_bv_growth         is 'Referring value for spar.seedlot.smp_mean_bv_growth column';
comment on column spar.seedlot_audit.area_of_use_comment        is 'Referring value for spar.seedlot.area_of_use_comment column';

/*
-- Function to fix Insert/Update/Delete staments executed on seedlot the table.
-- Should be used in a trigger on the seedlot table
-- trigger statement in the bottom part of this script
*/
CREATE OR REPLACE FUNCTION spar.seedlot_if_modified_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
  v_auditrevision int;
BEGIN
    if (TG_OP = 'UPDATE') then
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'U',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,
    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    elsif (TG_OP = 'DELETE') then
        v_old_data := ROW(OLD.*);
    /* insert into spar.logged_actions (schema_name,table_name,user_name,action,original_data,query)
        values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data, current_query());*/
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number) ;
    INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'D',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ OLD.seedlot_number,
    /*seedlot_status_code            */ OLD.seedlot_status_code,
    /*applicant_client_number        */ OLD.applicant_client_number,
    /*applicant_locn_code            */ OLD.applicant_locn_code,
    /*applicant_email_address        */ OLD.applicant_email_address,
    /*vegetation_code                */ OLD.vegetation_code,
    /*genetic_class_code             */ OLD.genetic_class_code,
    /*seedlot_source_code            */ OLD.seedlot_source_code,
    /*to_be_registrd_ind             */ OLD.to_be_registrd_ind,
    /*bc_source_ind                  */ OLD.bc_source_ind,
    /*collection_client_number       */ OLD.collection_client_number,
    /*collection_locn_code           */ OLD.collection_locn_code,
    /*collection_start_date          */ OLD.collection_start_date,
    /*collection_end_date            */ OLD.collection_end_date,
    /*no_of_containers               */ OLD.no_of_containers,
    /*vol_per_container              */ OLD.vol_per_container,
    /*clctn_volume                   */ OLD.clctn_volume,
    /*seedlot_comment                */ OLD.seedlot_comment,
    /*interm_strg_client_number      */ OLD.interm_strg_client_number,
    /*interm_strg_locn_code          */ OLD.interm_strg_locn_code,
    /*interm_strg_st_date            */ OLD.interm_strg_st_date,
    /*interm_strg_end_date           */ OLD.interm_strg_end_date,
    /*interm_facility_code           */ OLD.interm_facility_code,
    /*female_gametic_mthd_code       */ OLD.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ OLD.male_gametic_mthd_code,
    /*controlled_cross_ind           */ OLD.controlled_cross_ind,
    /*biotech_processes_ind          */ OLD.biotech_processes_ind,
    /*pollen_contamination_ind       */ OLD.pollen_contamination_ind,
    /*pollen_contamination_pct       */ OLD.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ OLD.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ OLD.pollen_contamination_mthd_code,
    /*total_parent_trees             */ OLD.total_parent_trees,
    /*smp_success_pct                */ OLD.smp_success_pct,
    /*effective_pop_size             */ OLD.effective_pop_size,
    /*smp_parents_outside            */ OLD.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ OLD.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ OLD.extractory_client_number,
    /*extractory_locn_code           */ OLD.extractory_locn_code,
    /*extraction_st_date             */ OLD.extraction_st_date,
    /*extraction_end_date            */ OLD.extraction_end_date,
    /*temporary_strg_client_number   */ OLD.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ OLD.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ OLD.temporary_strg_start_date,
    /*temporary_strg_end_date        */ OLD.temporary_strg_end_date,
    /*interm_strg_locn               */ OLD.interm_strg_locn,
    /*declared_userid                */ OLD.declared_userid,
    /*declared_timestamp             */ OLD.declared_timestamp,
    /*entry_userid                   */ OLD.entry_userid,
    /*entry_timestamp                */ OLD.entry_timestamp,
    /*update_userid                  */ OLD.update_userid,
    /*update_timestamp               */ OLD.update_timestamp,
    /*revision_count                 */ OLD.revision_count
    );
        RETURN OLD;
    elsif (TG_OP = 'INSERT') then
        v_new_data := ROW(NEW.*);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'I',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ 1, -- 1st row version
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,
    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    else
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
        RETURN NULL;
    end if;

EXCEPTION
    WHEN data_exception then
        --insert into spar.error_catch (erro) VALUES(CONCAT('DATA EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN unique_violation then
      --insert into spar.error_catch (erro) VALUES(CONCAT('UNIQUE EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN others then
        --insert into spar.error_catch (erro) VALUES(CONCAT(v_auditrevision, CONCAT('OTHER EXCEPTION ',SQLERRM)));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, spar;

-- Drop existing trigger
DROP TRIGGER trg_seedlot_audit_DIU ON spar.seedlot;

-- Trigger to be attached on spar.seedlot table
CREATE TRIGGER trg_seedlot_audit_DIU
 AFTER INSERT OR UPDATE OR DELETE ON spar.seedlot
 FOR EACH ROW EXECUTE PROCEDURE spar.seedlot_if_modified_func();

alter table spar.seedlot
  add column approved_timestamp     timestamp,
  add column approved_userid        varchar(30);

comment on column spar.seedlot_audit.seed_plan_unit_id          is 'Referring value for spar.seedlot.approved_timestamp column';
comment on column spar.seedlot_audit.bgc_zone_code              is 'Referring value for spar.seedlot.approved_userid column';

alter table spar.seedlot_audit
  add column approved_timestamp     timestamp,
  add column approved_userid        varchar(30);

comment on column spar.seedlot_audit.approved_timestamp          is 'Referring value for spar.seedlot.approved_timestamp column';
comment on column spar.seedlot_audit.approved_userid              is 'Referring value for spar.seedlot.approved_userid column';

/*
-- Function to fix Insert/Update/Delete staments executed on seedlot the table.
-- Should be used in a trigger on the seedlot table
-- trigger statement in the bottom part of this script
*/


CREATE OR REPLACE FUNCTION spar.seedlot_if_modified_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
  v_auditrevision int;
BEGIN
    if (TG_OP = 'UPDATE') then
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'U',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,

    /*seed_plan_unit_id              */ NEW.seed_plan_unit_id,
    /*bgc_zone_code                  */ NEW.bgc_zone_code,
    /*bgc_subzone_code               */ NEW.bgc_subzone_code,
    /*variant                        */ NEW.variant,
    /*bec_version_id                 */ NEW.bec_version_id,
    /*elevation                      */ NEW.elevation,
    /*latitude_degrees               */ NEW.latitude_degrees,
    /*latitude_minutes               */ NEW.latitude_minutes,
    /*latitude_seconds               */ NEW.latitude_seconds,
    /*longitude_degrees              */ NEW.longitude_degrees,
    /*longitude_minutes              */ NEW.longitude_minutes,
    /*longitude_seconds              */ NEW.longitude_seconds,
    /*collection_elevation           */ NEW.collection_elevation,
    /*collection_elevation_min       */ NEW.collection_elevation_min,
    /*collection_elevation_max       */ NEW.collection_elevation_max,
    /*collection_latitude_deg        */ NEW.collection_latitude_deg,
    /*collection_latitude_min        */ NEW.collection_latitude_min,
    /*collection_latitude_sec        */ NEW.collection_latitude_sec,
    /*collection_latitude_code       */ NEW.collection_latitude_code,
    /*collection_longitude_deg       */ NEW.collection_longitude_deg,
    /*collection_longitude_min       */ NEW.collection_longitude_min,
    /*collection_longitude_sec       */ NEW.collection_longitude_sec,
    /*collection_longitude_code      */ NEW.collection_longitude_code,
    /*elevation_min                  */ NEW.elevation_min,
    /*elevation_max                  */ NEW.elevation_max,
    /*latitude_deg_min               */ NEW.latitude_deg_min,
    /*latitude_min_min               */ NEW.latitude_min_min,
    /*latitude_sec_min               */ NEW.latitude_sec_min,
    /*latitude_deg_max               */ NEW.latitude_deg_max,
    /*latitude_min_max               */ NEW.latitude_min_max,
    /*latitude_sec_max               */ NEW.latitude_sec_max,
    /*longitude_deg_min              */ NEW.longitude_deg_min,
    /*longitude_min_min              */ NEW.longitude_min_min,
    /*longitude_sec_min              */ NEW.longitude_sec_min,
    /*longitude_deg_max              */ NEW.longitude_deg_max,
    /*longitude_min_max              */ NEW.longitude_min_max,
    /*longitude_sec_max              */ NEW.longitude_sec_max,
    /*smp_mean_bv_growth             */ NEW.smp_mean_bv_growth,
    /*area_of_use_comment            */ NEW.area_of_use_comment,
    /*approved_timestamp             */ NEW.approved_timestamp,
    /*approved_userid                */ NEW.approved_userid,

    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    elsif (TG_OP = 'DELETE') then
        v_old_data := ROW(OLD.*);
    /* insert into spar.logged_actions (schema_name,table_name,user_name,action,original_data,query)
        values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data, current_query());*/
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number) ;
    INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'D',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ OLD.seedlot_number,
    /*seedlot_status_code            */ OLD.seedlot_status_code,
    /*applicant_client_number        */ OLD.applicant_client_number,
    /*applicant_locn_code            */ OLD.applicant_locn_code,
    /*applicant_email_address        */ OLD.applicant_email_address,
    /*vegetation_code                */ OLD.vegetation_code,
    /*genetic_class_code             */ OLD.genetic_class_code,
    /*seedlot_source_code            */ OLD.seedlot_source_code,
    /*to_be_registrd_ind             */ OLD.to_be_registrd_ind,
    /*bc_source_ind                  */ OLD.bc_source_ind,
    /*collection_client_number       */ OLD.collection_client_number,
    /*collection_locn_code           */ OLD.collection_locn_code,
    /*collection_start_date          */ OLD.collection_start_date,
    /*collection_end_date            */ OLD.collection_end_date,
    /*no_of_containers               */ OLD.no_of_containers,
    /*vol_per_container              */ OLD.vol_per_container,
    /*clctn_volume                   */ OLD.clctn_volume,
    /*seedlot_comment                */ OLD.seedlot_comment,
    /*interm_strg_client_number      */ OLD.interm_strg_client_number,
    /*interm_strg_locn_code          */ OLD.interm_strg_locn_code,
    /*interm_strg_st_date            */ OLD.interm_strg_st_date,
    /*interm_strg_end_date           */ OLD.interm_strg_end_date,
    /*interm_facility_code           */ OLD.interm_facility_code,
    /*female_gametic_mthd_code       */ OLD.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ OLD.male_gametic_mthd_code,
    /*controlled_cross_ind           */ OLD.controlled_cross_ind,
    /*biotech_processes_ind          */ OLD.biotech_processes_ind,
    /*pollen_contamination_ind       */ OLD.pollen_contamination_ind,
    /*pollen_contamination_pct       */ OLD.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ OLD.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ OLD.pollen_contamination_mthd_code,
    /*total_parent_trees             */ OLD.total_parent_trees,
    /*smp_success_pct                */ OLD.smp_success_pct,
    /*effective_pop_size             */ OLD.effective_pop_size,
    /*smp_parents_outside            */ OLD.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ OLD.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ OLD.extractory_client_number,
    /*extractory_locn_code           */ OLD.extractory_locn_code,
    /*extraction_st_date             */ OLD.extraction_st_date,
    /*extraction_end_date            */ OLD.extraction_end_date,
    /*temporary_strg_client_number   */ OLD.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ OLD.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ OLD.temporary_strg_start_date,
    /*temporary_strg_end_date        */ OLD.temporary_strg_end_date,
    /*interm_strg_locn               */ OLD.interm_strg_locn,
    /*declared_userid                */ OLD.declared_userid,
    /*declared_timestamp             */ OLD.declared_timestamp,
    /*entry_userid                   */ OLD.entry_userid,
    /*entry_timestamp                */ OLD.entry_timestamp,

	/*seed_plan_unit_id              */ OLD.seed_plan_unit_id,
    /*bgc_zone_code                  */ OLD.bgc_zone_code,
    /*bgc_subzone_code               */ OLD.bgc_subzone_code,
    /*variant                        */ OLD.variant,
    /*bec_version_id                 */ OLD.bec_version_id,
    /*elevation                      */ OLD.elevation,
    /*latitude_degrees               */ OLD.latitude_degrees,
    /*latitude_minutes               */ OLD.latitude_minutes,
    /*latitude_seconds               */ OLD.latitude_seconds,
    /*longitude_degrees              */ OLD.longitude_degrees,
    /*longitude_minutes              */ OLD.longitude_minutes,
    /*longitude_seconds              */ OLD.longitude_seconds,
    /*collection_elevation           */ OLD.collection_elevation,
    /*collection_elevation_min       */ OLD.collection_elevation_min,
    /*collection_elevation_max       */ OLD.collection_elevation_max,
    /*collection_latitude_deg        */ OLD.collection_latitude_deg,
    /*collection_latitude_min        */ OLD.collection_latitude_min,
    /*collection_latitude_sec        */ OLD.collection_latitude_sec,
    /*collection_latitude_code       */ OLD.collection_latitude_code,
    /*collection_longitude_deg       */ OLD.collection_longitude_deg,
    /*collection_longitude_min       */ OLD.collection_longitude_min,
    /*collection_longitude_sec       */ OLD.collection_longitude_sec,
    /*collection_longitude_code      */ OLD.collection_longitude_code,
    /*elevation_min                  */ OLD.elevation_min,
    /*elevation_max                  */ OLD.elevation_max,
    /*latitude_deg_min               */ OLD.latitude_deg_min,
    /*latitude_min_min               */ OLD.latitude_min_min,
    /*latitude_sec_min               */ OLD.latitude_sec_min,
    /*latitude_deg_max               */ OLD.latitude_deg_max,
    /*latitude_min_max               */ OLD.latitude_min_max,
    /*latitude_sec_max               */ OLD.latitude_sec_max,
    /*longitude_deg_min              */ OLD.longitude_deg_min,
    /*longitude_min_min              */ OLD.longitude_min_min,
    /*longitude_sec_min              */ OLD.longitude_sec_min,
    /*longitude_deg_max              */ OLD.longitude_deg_max,
    /*longitude_min_max              */ OLD.longitude_min_max,
    /*longitude_sec_max              */ OLD.longitude_sec_max,
    /*smp_mean_bv_growth             */ OLD.smp_mean_bv_growth,
    /*area_of_use_comment            */ OLD.area_of_use_comment,
    /*approved_timestamp             */ OLD.approved_timestamp,
    /*approved_userid                */ OLD.approved_userid,

    /*update_userid                  */ OLD.update_userid,
    /*update_timestamp               */ OLD.update_timestamp,
    /*revision_count                 */ OLD.revision_count
    );
        RETURN OLD;
    elsif (TG_OP = 'INSERT') then
        v_new_data := ROW(NEW.*);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'I',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ 1, -- 1st row version
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,

	/*seed_plan_unit_id              */ NEW.seed_plan_unit_id,
    /*bgc_zone_code                  */ NEW.bgc_zone_code,
    /*bgc_subzone_code               */ NEW.bgc_subzone_code,
    /*variant                        */ NEW.variant,
    /*bec_version_id                 */ NEW.bec_version_id,
    /*elevation                      */ NEW.elevation,
    /*latitude_degrees               */ NEW.latitude_degrees,
    /*latitude_minutes               */ NEW.latitude_minutes,
    /*latitude_seconds               */ NEW.latitude_seconds,
    /*longitude_degrees              */ NEW.longitude_degrees,
    /*longitude_minutes              */ NEW.longitude_minutes,
    /*longitude_seconds              */ NEW.longitude_seconds,
    /*collection_elevation           */ NEW.collection_elevation,
    /*collection_elevation_min       */ NEW.collection_elevation_min,
    /*collection_elevation_max       */ NEW.collection_elevation_max,
    /*collection_latitude_deg        */ NEW.collection_latitude_deg,
    /*collection_latitude_min        */ NEW.collection_latitude_min,
    /*collection_latitude_sec        */ NEW.collection_latitude_sec,
    /*collection_latitude_code       */ NEW.collection_latitude_code,
    /*collection_longitude_deg       */ NEW.collection_longitude_deg,
    /*collection_longitude_min       */ NEW.collection_longitude_min,
    /*collection_longitude_sec       */ NEW.collection_longitude_sec,
    /*collection_longitude_code      */ NEW.collection_longitude_code,
    /*elevation_min                  */ NEW.elevation_min,
    /*elevation_max                  */ NEW.elevation_max,
    /*latitude_deg_min               */ NEW.latitude_deg_min,
    /*latitude_min_min               */ NEW.latitude_min_min,
    /*latitude_sec_min               */ NEW.latitude_sec_min,
    /*latitude_deg_max               */ NEW.latitude_deg_max,
    /*latitude_min_max               */ NEW.latitude_min_max,
    /*latitude_sec_max               */ NEW.latitude_sec_max,
    /*longitude_deg_min              */ NEW.longitude_deg_min,
    /*longitude_min_min              */ NEW.longitude_min_min,
    /*longitude_sec_min              */ NEW.longitude_sec_min,
    /*longitude_deg_max              */ NEW.longitude_deg_max,
    /*longitude_min_max              */ NEW.longitude_min_max,
    /*longitude_sec_max              */ NEW.longitude_sec_max,
    /*smp_mean_bv_growth             */ NEW.smp_mean_bv_growth,
    /*area_of_use_comment            */ NEW.area_of_use_comment,
    /*approved_timestamp             */ NEW.approved_timestamp,
    /*approved_userid                */ NEW.approved_userid,

    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    else
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
        RETURN NULL;
    end if;

EXCEPTION
    WHEN data_exception then
        --insert into spar.error_catch (erro) VALUES(CONCAT('DATA EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN unique_violation then
      --insert into spar.error_catch (erro) VALUES(CONCAT('UNIQUE EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN others then
        --insert into spar.error_catch (erro) VALUES(CONCAT(v_auditrevision, CONCAT('OTHER EXCEPTION ',SQLERRM)));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, spar;

---- V_28

comment on column spar.seedlot_audit.seed_plan_unit_id          is 'Referring value for spar.seedlot.seed_plan_unit_id column';
comment on column spar.seedlot_audit.bgc_zone_code              is 'Referring value for spar.seedlot.bgc_zone_code column';
comment on column spar.seedlot_audit.bgc_subzone_code           is 'Referring value for spar.seedlot.bgc_subzone_code column';
comment on column spar.seedlot_audit.variant                    is 'Referring value for spar.seedlot.variant column';
comment on column spar.seedlot_audit.bec_version_id             is 'Referring value for spar.seedlot.bec_version_id column';
comment on column spar.seedlot_audit.elevation                  is 'Referring value for spar.seedlot.elevation column';
comment on column spar.seedlot_audit.latitude_degrees           is 'Referring value for spar.seedlot.latitude_degrees column';
comment on column spar.seedlot_audit.latitude_minutes           is 'Referring value for spar.seedlot.latitude_minutes column';
comment on column spar.seedlot_audit.latitude_seconds           is 'Referring value for spar.seedlot.latitude_seconds column';
comment on column spar.seedlot_audit.longitude_degrees          is 'Referring value for spar.seedlot.longitude_degrees column';
comment on column spar.seedlot_audit.longitude_minutes          is 'Referring value for spar.seedlot.longitude_minutes column';
comment on column spar.seedlot_audit.longitude_seconds          is 'Referring value for spar.seedlot.longitude_seconds column';
comment on column spar.seedlot_audit.collection_elevation       is 'Referring value for spar.seedlot.collection_elevation column';
comment on column spar.seedlot_audit.collection_elevation_min   is 'Referring value for spar.seedlot.collection_elevation_min column';
comment on column spar.seedlot_audit.collection_elevation_max   is 'Referring value for spar.seedlot.collection_elevation_max column';
comment on column spar.seedlot_audit.collection_latitude_deg    is 'Referring value for spar.seedlot.collection_latitude_deg column';
comment on column spar.seedlot_audit.collection_latitude_min    is 'Referring value for spar.seedlot.collection_latitude_min column';
comment on column spar.seedlot_audit.collection_latitude_sec    is 'Referring value for spar.seedlot.collection_latitude_sec column';
comment on column spar.seedlot_audit.collection_latitude_code   is 'Referring value for spar.seedlot.collection_latitude_code column';
comment on column spar.seedlot_audit.collection_longitude_deg   is 'Referring value for spar.seedlot.collection_longitude_deg column';
comment on column spar.seedlot_audit.collection_longitude_min   is 'Referring value for spar.seedlot.collection_longitude_min column';
comment on column spar.seedlot_audit.collection_longitude_sec   is 'Referring value for spar.seedlot.collection_longitude_sec column';
comment on column spar.seedlot_audit.collection_longitude_code  is 'Referring value for spar.seedlot.collection_longitude_code column';
comment on column spar.seedlot_audit.elevation_min              is 'Referring value for spar.seedlot.elevation_min column';
comment on column spar.seedlot_audit.elevation_max              is 'Referring value for spar.seedlot.elevation_max column';
comment on column spar.seedlot_audit.latitude_deg_min           is 'Referring value for spar.seedlot.latitude_deg_min column';
comment on column spar.seedlot_audit.latitude_sec_min           is 'Referring value for spar.seedlot.latitude_sec_min column';
comment on column spar.seedlot_audit.latitude_min_min           is 'Referring value for spar.seedlot.latitude_min_min column';
comment on column spar.seedlot_audit.latitude_deg_max           is 'Referring value for spar.seedlot.latitude_deg_max column';
comment on column spar.seedlot_audit.latitude_min_max           is 'Referring value for spar.seedlot.latitude_min_max column';
comment on column spar.seedlot_audit.latitude_sec_max           is 'Referring value for spar.seedlot.latitude_sec_max column';
comment on column spar.seedlot_audit.longitude_deg_min          is 'Referring value for spar.seedlot.longitude_deg_min column';
comment on column spar.seedlot_audit.longitude_min_min          is 'Referring value for spar.seedlot.longitude_min_min column';
comment on column spar.seedlot_audit.longitude_sec_min          is 'Referring value for spar.seedlot.longitude_sec_min column';
comment on column spar.seedlot_audit.longitude_deg_max          is 'Referring value for spar.seedlot.longitude_deg_max column';
comment on column spar.seedlot_audit.longitude_min_max          is 'Referring value for spar.seedlot.longitude_min_max column';
comment on column spar.seedlot_audit.longitude_sec_max          is 'Referring value for spar.seedlot.longitude_sec_max column';
comment on column spar.seedlot_audit.smp_mean_bv_growth         is 'Referring value for spar.seedlot.smp_mean_bv_growth column';
comment on column spar.seedlot_audit.area_of_use_comment        is 'Referring value for spar.seedlot.area_of_use_comment column';

/*
-- Function to fix Insert/Update/Delete staments executed on seedlot the table.
-- Should be used in a trigger on the seedlot table
-- trigger statement in the bottom part of this script
*/
CREATE OR REPLACE FUNCTION spar.seedlot_if_modified_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
  v_auditrevision int;
BEGIN
    if (TG_OP = 'UPDATE') then
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'U',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,
    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    elsif (TG_OP = 'DELETE') then
        v_old_data := ROW(OLD.*);
    /* insert into spar.logged_actions (schema_name,table_name,user_name,action,original_data,query)
        values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data, current_query());*/
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number) ;
    INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'D',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ OLD.seedlot_number,
    /*seedlot_status_code            */ OLD.seedlot_status_code,
    /*applicant_client_number        */ OLD.applicant_client_number,
    /*applicant_locn_code            */ OLD.applicant_locn_code,
    /*applicant_email_address        */ OLD.applicant_email_address,
    /*vegetation_code                */ OLD.vegetation_code,
    /*genetic_class_code             */ OLD.genetic_class_code,
    /*seedlot_source_code            */ OLD.seedlot_source_code,
    /*to_be_registrd_ind             */ OLD.to_be_registrd_ind,
    /*bc_source_ind                  */ OLD.bc_source_ind,
    /*collection_client_number       */ OLD.collection_client_number,
    /*collection_locn_code           */ OLD.collection_locn_code,
    /*collection_start_date          */ OLD.collection_start_date,
    /*collection_end_date            */ OLD.collection_end_date,
    /*no_of_containers               */ OLD.no_of_containers,
    /*vol_per_container              */ OLD.vol_per_container,
    /*clctn_volume                   */ OLD.clctn_volume,
    /*seedlot_comment                */ OLD.seedlot_comment,
    /*interm_strg_client_number      */ OLD.interm_strg_client_number,
    /*interm_strg_locn_code          */ OLD.interm_strg_locn_code,
    /*interm_strg_st_date            */ OLD.interm_strg_st_date,
    /*interm_strg_end_date           */ OLD.interm_strg_end_date,
    /*interm_facility_code           */ OLD.interm_facility_code,
    /*female_gametic_mthd_code       */ OLD.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ OLD.male_gametic_mthd_code,
    /*controlled_cross_ind           */ OLD.controlled_cross_ind,
    /*biotech_processes_ind          */ OLD.biotech_processes_ind,
    /*pollen_contamination_ind       */ OLD.pollen_contamination_ind,
    /*pollen_contamination_pct       */ OLD.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ OLD.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ OLD.pollen_contamination_mthd_code,
    /*total_parent_trees             */ OLD.total_parent_trees,
    /*smp_success_pct                */ OLD.smp_success_pct,
    /*effective_pop_size             */ OLD.effective_pop_size,
    /*smp_parents_outside            */ OLD.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ OLD.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ OLD.extractory_client_number,
    /*extractory_locn_code           */ OLD.extractory_locn_code,
    /*extraction_st_date             */ OLD.extraction_st_date,
    /*extraction_end_date            */ OLD.extraction_end_date,
    /*temporary_strg_client_number   */ OLD.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ OLD.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ OLD.temporary_strg_start_date,
    /*temporary_strg_end_date        */ OLD.temporary_strg_end_date,
    /*interm_strg_locn               */ OLD.interm_strg_locn,
    /*declared_userid                */ OLD.declared_userid,
    /*declared_timestamp             */ OLD.declared_timestamp,
    /*entry_userid                   */ OLD.entry_userid,
    /*entry_timestamp                */ OLD.entry_timestamp,
    /*update_userid                  */ OLD.update_userid,
    /*update_timestamp               */ OLD.update_timestamp,
    /*revision_count                 */ OLD.revision_count
    );
        RETURN OLD;
    elsif (TG_OP = 'INSERT') then
        v_new_data := ROW(NEW.*);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'I',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ 1, -- 1st row version
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,
    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    else
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
        RETURN NULL;
    end if;

EXCEPTION
    WHEN data_exception then
        --insert into spar.error_catch (erro) VALUES(CONCAT('DATA EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN unique_violation then
      --insert into spar.error_catch (erro) VALUES(CONCAT('UNIQUE EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN others then
        --insert into spar.error_catch (erro) VALUES(CONCAT(v_auditrevision, CONCAT('OTHER EXCEPTION ',SQLERRM)));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, spar;

-- Drop existing trigger
DROP TRIGGER trg_seedlot_audit_DIU ON spar.seedlot;

-- Trigger to be attached on spar.seedlot table
CREATE TRIGGER trg_seedlot_audit_DIU
 AFTER INSERT OR UPDATE OR DELETE ON spar.seedlot
 FOR EACH ROW EXECUTE PROCEDURE spar.seedlot_if_modified_func();

comment on column spar.seedlot_audit.seed_plan_unit_id          is 'Referring value for spar.seedlot.approved_timestamp column';
comment on column spar.seedlot_audit.bgc_zone_code              is 'Referring value for spar.seedlot.approved_userid column';
comment on column spar.seedlot_audit.approved_timestamp          is 'Referring value for spar.seedlot.approved_timestamp column';
comment on column spar.seedlot_audit.approved_userid              is 'Referring value for spar.seedlot.approved_userid column';

/*
-- Function to fix Insert/Update/Delete staments executed on seedlot the table.
-- Should be used in a trigger on the seedlot table
-- trigger statement in the bottom part of this script
*/


CREATE OR REPLACE FUNCTION spar.seedlot_if_modified_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
  v_auditrevision int;
BEGIN
    if (TG_OP = 'UPDATE') then
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'U',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,

    /*seed_plan_unit_id              */ NEW.seed_plan_unit_id,
    /*bgc_zone_code                  */ NEW.bgc_zone_code,
    /*bgc_subzone_code               */ NEW.bgc_subzone_code,
    /*variant                        */ NEW.variant,
    /*bec_version_id                 */ NEW.bec_version_id,
    /*elevation                      */ NEW.elevation,
    /*latitude_degrees               */ NEW.latitude_degrees,
    /*latitude_minutes               */ NEW.latitude_minutes,
    /*latitude_seconds               */ NEW.latitude_seconds,
    /*longitude_degrees              */ NEW.longitude_degrees,
    /*longitude_minutes              */ NEW.longitude_minutes,
    /*longitude_seconds              */ NEW.longitude_seconds,
    /*collection_elevation           */ NEW.collection_elevation,
    /*collection_elevation_min       */ NEW.collection_elevation_min,
    /*collection_elevation_max       */ NEW.collection_elevation_max,
    /*collection_latitude_deg        */ NEW.collection_latitude_deg,
    /*collection_latitude_min        */ NEW.collection_latitude_min,
    /*collection_latitude_sec        */ NEW.collection_latitude_sec,
    /*collection_latitude_code       */ NEW.collection_latitude_code,
    /*collection_longitude_deg       */ NEW.collection_longitude_deg,
    /*collection_longitude_min       */ NEW.collection_longitude_min,
    /*collection_longitude_sec       */ NEW.collection_longitude_sec,
    /*collection_longitude_code      */ NEW.collection_longitude_code,
    /*elevation_min                  */ NEW.elevation_min,
    /*elevation_max                  */ NEW.elevation_max,
    /*latitude_deg_min               */ NEW.latitude_deg_min,
    /*latitude_min_min               */ NEW.latitude_min_min,
    /*latitude_sec_min               */ NEW.latitude_sec_min,
    /*latitude_deg_max               */ NEW.latitude_deg_max,
    /*latitude_min_max               */ NEW.latitude_min_max,
    /*latitude_sec_max               */ NEW.latitude_sec_max,
    /*longitude_deg_min              */ NEW.longitude_deg_min,
    /*longitude_min_min              */ NEW.longitude_min_min,
    /*longitude_sec_min              */ NEW.longitude_sec_min,
    /*longitude_deg_max              */ NEW.longitude_deg_max,
    /*longitude_min_max              */ NEW.longitude_min_max,
    /*longitude_sec_max              */ NEW.longitude_sec_max,
    /*smp_mean_bv_growth             */ NEW.smp_mean_bv_growth,
    /*area_of_use_comment            */ NEW.area_of_use_comment,
    /*approved_timestamp             */ NEW.approved_timestamp,
    /*approved_userid                */ NEW.approved_userid,

    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    elsif (TG_OP = 'DELETE') then
        v_old_data := ROW(OLD.*);
    /* insert into spar.logged_actions (schema_name,table_name,user_name,action,original_data,query)
        values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data, current_query());*/
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number) ;
    INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'D',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ OLD.seedlot_number,
    /*seedlot_status_code            */ OLD.seedlot_status_code,
    /*applicant_client_number        */ OLD.applicant_client_number,
    /*applicant_locn_code            */ OLD.applicant_locn_code,
    /*applicant_email_address        */ OLD.applicant_email_address,
    /*vegetation_code                */ OLD.vegetation_code,
    /*genetic_class_code             */ OLD.genetic_class_code,
    /*seedlot_source_code            */ OLD.seedlot_source_code,
    /*to_be_registrd_ind             */ OLD.to_be_registrd_ind,
    /*bc_source_ind                  */ OLD.bc_source_ind,
    /*collection_client_number       */ OLD.collection_client_number,
    /*collection_locn_code           */ OLD.collection_locn_code,
    /*collection_start_date          */ OLD.collection_start_date,
    /*collection_end_date            */ OLD.collection_end_date,
    /*no_of_containers               */ OLD.no_of_containers,
    /*vol_per_container              */ OLD.vol_per_container,
    /*clctn_volume                   */ OLD.clctn_volume,
    /*seedlot_comment                */ OLD.seedlot_comment,
    /*interm_strg_client_number      */ OLD.interm_strg_client_number,
    /*interm_strg_locn_code          */ OLD.interm_strg_locn_code,
    /*interm_strg_st_date            */ OLD.interm_strg_st_date,
    /*interm_strg_end_date           */ OLD.interm_strg_end_date,
    /*interm_facility_code           */ OLD.interm_facility_code,
    /*female_gametic_mthd_code       */ OLD.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ OLD.male_gametic_mthd_code,
    /*controlled_cross_ind           */ OLD.controlled_cross_ind,
    /*biotech_processes_ind          */ OLD.biotech_processes_ind,
    /*pollen_contamination_ind       */ OLD.pollen_contamination_ind,
    /*pollen_contamination_pct       */ OLD.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ OLD.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ OLD.pollen_contamination_mthd_code,
    /*total_parent_trees             */ OLD.total_parent_trees,
    /*smp_success_pct                */ OLD.smp_success_pct,
    /*effective_pop_size             */ OLD.effective_pop_size,
    /*smp_parents_outside            */ OLD.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ OLD.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ OLD.extractory_client_number,
    /*extractory_locn_code           */ OLD.extractory_locn_code,
    /*extraction_st_date             */ OLD.extraction_st_date,
    /*extraction_end_date            */ OLD.extraction_end_date,
    /*temporary_strg_client_number   */ OLD.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ OLD.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ OLD.temporary_strg_start_date,
    /*temporary_strg_end_date        */ OLD.temporary_strg_end_date,
    /*interm_strg_locn               */ OLD.interm_strg_locn,
    /*declared_userid                */ OLD.declared_userid,
    /*declared_timestamp             */ OLD.declared_timestamp,
    /*entry_userid                   */ OLD.entry_userid,
    /*entry_timestamp                */ OLD.entry_timestamp,

	/*seed_plan_unit_id              */ OLD.seed_plan_unit_id,
    /*bgc_zone_code                  */ OLD.bgc_zone_code,
    /*bgc_subzone_code               */ OLD.bgc_subzone_code,
    /*variant                        */ OLD.variant,
    /*bec_version_id                 */ OLD.bec_version_id,
    /*elevation                      */ OLD.elevation,
    /*latitude_degrees               */ OLD.latitude_degrees,
    /*latitude_minutes               */ OLD.latitude_minutes,
    /*latitude_seconds               */ OLD.latitude_seconds,
    /*longitude_degrees              */ OLD.longitude_degrees,
    /*longitude_minutes              */ OLD.longitude_minutes,
    /*longitude_seconds              */ OLD.longitude_seconds,
    /*collection_elevation           */ OLD.collection_elevation,
    /*collection_elevation_min       */ OLD.collection_elevation_min,
    /*collection_elevation_max       */ OLD.collection_elevation_max,
    /*collection_latitude_deg        */ OLD.collection_latitude_deg,
    /*collection_latitude_min        */ OLD.collection_latitude_min,
    /*collection_latitude_sec        */ OLD.collection_latitude_sec,
    /*collection_latitude_code       */ OLD.collection_latitude_code,
    /*collection_longitude_deg       */ OLD.collection_longitude_deg,
    /*collection_longitude_min       */ OLD.collection_longitude_min,
    /*collection_longitude_sec       */ OLD.collection_longitude_sec,
    /*collection_longitude_code      */ OLD.collection_longitude_code,
    /*elevation_min                  */ OLD.elevation_min,
    /*elevation_max                  */ OLD.elevation_max,
    /*latitude_deg_min               */ OLD.latitude_deg_min,
    /*latitude_min_min               */ OLD.latitude_min_min,
    /*latitude_sec_min               */ OLD.latitude_sec_min,
    /*latitude_deg_max               */ OLD.latitude_deg_max,
    /*latitude_min_max               */ OLD.latitude_min_max,
    /*latitude_sec_max               */ OLD.latitude_sec_max,
    /*longitude_deg_min              */ OLD.longitude_deg_min,
    /*longitude_min_min              */ OLD.longitude_min_min,
    /*longitude_sec_min              */ OLD.longitude_sec_min,
    /*longitude_deg_max              */ OLD.longitude_deg_max,
    /*longitude_min_max              */ OLD.longitude_min_max,
    /*longitude_sec_max              */ OLD.longitude_sec_max,
    /*smp_mean_bv_growth             */ OLD.smp_mean_bv_growth,
    /*area_of_use_comment            */ OLD.area_of_use_comment,
    /*approved_timestamp             */ OLD.approved_timestamp,
    /*approved_userid                */ OLD.approved_userid,

    /*update_userid                  */ OLD.update_userid,
    /*update_timestamp               */ OLD.update_timestamp,
    /*revision_count                 */ OLD.revision_count
    );
        RETURN OLD;
    elsif (TG_OP = 'INSERT') then
        v_new_data := ROW(NEW.*);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'I',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ 1, -- 1st row version
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,

	/*seed_plan_unit_id              */ NEW.seed_plan_unit_id,
    /*bgc_zone_code                  */ NEW.bgc_zone_code,
    /*bgc_subzone_code               */ NEW.bgc_subzone_code,
    /*variant                        */ NEW.variant,
    /*bec_version_id                 */ NEW.bec_version_id,
    /*elevation                      */ NEW.elevation,
    /*latitude_degrees               */ NEW.latitude_degrees,
    /*latitude_minutes               */ NEW.latitude_minutes,
    /*latitude_seconds               */ NEW.latitude_seconds,
    /*longitude_degrees              */ NEW.longitude_degrees,
    /*longitude_minutes              */ NEW.longitude_minutes,
    /*longitude_seconds              */ NEW.longitude_seconds,
    /*collection_elevation           */ NEW.collection_elevation,
    /*collection_elevation_min       */ NEW.collection_elevation_min,
    /*collection_elevation_max       */ NEW.collection_elevation_max,
    /*collection_latitude_deg        */ NEW.collection_latitude_deg,
    /*collection_latitude_min        */ NEW.collection_latitude_min,
    /*collection_latitude_sec        */ NEW.collection_latitude_sec,
    /*collection_latitude_code       */ NEW.collection_latitude_code,
    /*collection_longitude_deg       */ NEW.collection_longitude_deg,
    /*collection_longitude_min       */ NEW.collection_longitude_min,
    /*collection_longitude_sec       */ NEW.collection_longitude_sec,
    /*collection_longitude_code      */ NEW.collection_longitude_code,
    /*elevation_min                  */ NEW.elevation_min,
    /*elevation_max                  */ NEW.elevation_max,
    /*latitude_deg_min               */ NEW.latitude_deg_min,
    /*latitude_min_min               */ NEW.latitude_min_min,
    /*latitude_sec_min               */ NEW.latitude_sec_min,
    /*latitude_deg_max               */ NEW.latitude_deg_max,
    /*latitude_min_max               */ NEW.latitude_min_max,
    /*latitude_sec_max               */ NEW.latitude_sec_max,
    /*longitude_deg_min              */ NEW.longitude_deg_min,
    /*longitude_min_min              */ NEW.longitude_min_min,
    /*longitude_sec_min              */ NEW.longitude_sec_min,
    /*longitude_deg_max              */ NEW.longitude_deg_max,
    /*longitude_min_max              */ NEW.longitude_min_max,
    /*longitude_sec_max              */ NEW.longitude_sec_max,
    /*smp_mean_bv_growth             */ NEW.smp_mean_bv_growth,
    /*area_of_use_comment            */ NEW.area_of_use_comment,
    /*approved_timestamp             */ NEW.approved_timestamp,
    /*approved_userid                */ NEW.approved_userid,

    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    else
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
        RETURN NULL;
    end if;

EXCEPTION
    WHEN data_exception then
        --insert into spar.error_catch (erro) VALUES(CONCAT('DATA EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN unique_violation then
      --insert into spar.error_catch (erro) VALUES(CONCAT('UNIQUE EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN others then
        --insert into spar.error_catch (erro) VALUES(CONCAT(v_auditrevision, CONCAT('OTHER EXCEPTION ',SQLERRM)));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, spar;

/* Update Seedlot table */
--alter table spar.seedlot add column mean_geom geometry(POINTZ, 4326);

--comment on column spar.seedlot.mean_geom is 'The 3D mean geom of parent trees.';

/* Update Seedlot Audit table */
--alter table spar.seedlot_audit add column mean_geom geometry(POINTZ, 4326);

--comment on column spar.seedlot_audit.mean_geom is 'Referring value for spar.seedlot.mean_geom column';

/* Update trigger */

CREATE OR REPLACE FUNCTION spar.seedlot_if_modified_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
  v_auditrevision int;
BEGIN
    if (TG_OP = 'UPDATE') then
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'U',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,

    /*seed_plan_unit_id              */ NEW.seed_plan_unit_id,
    /*bgc_zone_code                  */ NEW.bgc_zone_code,
    /*bgc_subzone_code               */ NEW.bgc_subzone_code,
    /*variant                        */ NEW.variant,
    /*bec_version_id                 */ NEW.bec_version_id,
    /*elevation                      */ NEW.elevation,
    /*latitude_degrees               */ NEW.latitude_degrees,
    /*latitude_minutes               */ NEW.latitude_minutes,
    /*latitude_seconds               */ NEW.latitude_seconds,
    /*longitude_degrees              */ NEW.longitude_degrees,
    /*longitude_minutes              */ NEW.longitude_minutes,
    /*longitude_seconds              */ NEW.longitude_seconds,
    /*collection_elevation           */ NEW.collection_elevation,
    /*collection_elevation_min       */ NEW.collection_elevation_min,
    /*collection_elevation_max       */ NEW.collection_elevation_max,
    /*collection_latitude_deg        */ NEW.collection_latitude_deg,
    /*collection_latitude_min        */ NEW.collection_latitude_min,
    /*collection_latitude_sec        */ NEW.collection_latitude_sec,
    /*collection_latitude_code       */ NEW.collection_latitude_code,
    /*collection_longitude_deg       */ NEW.collection_longitude_deg,
    /*collection_longitude_min       */ NEW.collection_longitude_min,
    /*collection_longitude_sec       */ NEW.collection_longitude_sec,
    /*collection_longitude_code      */ NEW.collection_longitude_code,
    /*elevation_min                  */ NEW.elevation_min,
    /*elevation_max                  */ NEW.elevation_max,
    /*latitude_deg_min               */ NEW.latitude_deg_min,
    /*latitude_min_min               */ NEW.latitude_min_min,
    /*latitude_sec_min               */ NEW.latitude_sec_min,
    /*latitude_deg_max               */ NEW.latitude_deg_max,
    /*latitude_min_max               */ NEW.latitude_min_max,
    /*latitude_sec_max               */ NEW.latitude_sec_max,
    /*longitude_deg_min              */ NEW.longitude_deg_min,
    /*longitude_min_min              */ NEW.longitude_min_min,
    /*longitude_sec_min              */ NEW.longitude_sec_min,
    /*longitude_deg_max              */ NEW.longitude_deg_max,
    /*longitude_min_max              */ NEW.longitude_min_max,
    /*longitude_sec_max              */ NEW.longitude_sec_max,
    /*smp_mean_bv_growth             */ NEW.smp_mean_bv_growth,
    /*area_of_use_comment            */ NEW.area_of_use_comment,
    /*approved_timestamp             */ NEW.approved_timestamp,
    /*approved_userid                */ NEW.approved_userid,
    /*mean_geom                  */ --NEW.mean_geom,

    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    elsif (TG_OP = 'DELETE') then
        v_old_data := ROW(OLD.*);
    /* insert into spar.logged_actions (schema_name,table_name,user_name,action,original_data,query)
        values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data, current_query());*/
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number) ;
    INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'D',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ OLD.seedlot_number,
    /*seedlot_status_code            */ OLD.seedlot_status_code,
    /*applicant_client_number        */ OLD.applicant_client_number,
    /*applicant_locn_code            */ OLD.applicant_locn_code,
    /*applicant_email_address        */ OLD.applicant_email_address,
    /*vegetation_code                */ OLD.vegetation_code,
    /*genetic_class_code             */ OLD.genetic_class_code,
    /*seedlot_source_code            */ OLD.seedlot_source_code,
    /*to_be_registrd_ind             */ OLD.to_be_registrd_ind,
    /*bc_source_ind                  */ OLD.bc_source_ind,
    /*collection_client_number       */ OLD.collection_client_number,
    /*collection_locn_code           */ OLD.collection_locn_code,
    /*collection_start_date          */ OLD.collection_start_date,
    /*collection_end_date            */ OLD.collection_end_date,
    /*no_of_containers               */ OLD.no_of_containers,
    /*vol_per_container              */ OLD.vol_per_container,
    /*clctn_volume                   */ OLD.clctn_volume,
    /*seedlot_comment                */ OLD.seedlot_comment,
    /*interm_strg_client_number      */ OLD.interm_strg_client_number,
    /*interm_strg_locn_code          */ OLD.interm_strg_locn_code,
    /*interm_strg_st_date            */ OLD.interm_strg_st_date,
    /*interm_strg_end_date           */ OLD.interm_strg_end_date,
    /*interm_facility_code           */ OLD.interm_facility_code,
    /*female_gametic_mthd_code       */ OLD.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ OLD.male_gametic_mthd_code,
    /*controlled_cross_ind           */ OLD.controlled_cross_ind,
    /*biotech_processes_ind          */ OLD.biotech_processes_ind,
    /*pollen_contamination_ind       */ OLD.pollen_contamination_ind,
    /*pollen_contamination_pct       */ OLD.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ OLD.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ OLD.pollen_contamination_mthd_code,
    /*total_parent_trees             */ OLD.total_parent_trees,
    /*smp_success_pct                */ OLD.smp_success_pct,
    /*effective_pop_size             */ OLD.effective_pop_size,
    /*smp_parents_outside            */ OLD.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ OLD.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ OLD.extractory_client_number,
    /*extractory_locn_code           */ OLD.extractory_locn_code,
    /*extraction_st_date             */ OLD.extraction_st_date,
    /*extraction_end_date            */ OLD.extraction_end_date,
    /*temporary_strg_client_number   */ OLD.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ OLD.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ OLD.temporary_strg_start_date,
    /*temporary_strg_end_date        */ OLD.temporary_strg_end_date,
    /*interm_strg_locn               */ OLD.interm_strg_locn,
    /*declared_userid                */ OLD.declared_userid,
    /*declared_timestamp             */ OLD.declared_timestamp,
    /*entry_userid                   */ OLD.entry_userid,
    /*entry_timestamp                */ OLD.entry_timestamp,

	  /*seed_plan_unit_id              */ OLD.seed_plan_unit_id,
    /*bgc_zone_code                  */ OLD.bgc_zone_code,
    /*bgc_subzone_code               */ OLD.bgc_subzone_code,
    /*variant                        */ OLD.variant,
    /*bec_version_id                 */ OLD.bec_version_id,
    /*elevation                      */ OLD.elevation,
    /*latitude_degrees               */ OLD.latitude_degrees,
    /*latitude_minutes               */ OLD.latitude_minutes,
    /*latitude_seconds               */ OLD.latitude_seconds,
    /*longitude_degrees              */ OLD.longitude_degrees,
    /*longitude_minutes              */ OLD.longitude_minutes,
    /*longitude_seconds              */ OLD.longitude_seconds,
    /*collection_elevation           */ OLD.collection_elevation,
    /*collection_elevation_min       */ OLD.collection_elevation_min,
    /*collection_elevation_max       */ OLD.collection_elevation_max,
    /*collection_latitude_deg        */ OLD.collection_latitude_deg,
    /*collection_latitude_min        */ OLD.collection_latitude_min,
    /*collection_latitude_sec        */ OLD.collection_latitude_sec,
    /*collection_latitude_code       */ OLD.collection_latitude_code,
    /*collection_longitude_deg       */ OLD.collection_longitude_deg,
    /*collection_longitude_min       */ OLD.collection_longitude_min,
    /*collection_longitude_sec       */ OLD.collection_longitude_sec,
    /*collection_longitude_code      */ OLD.collection_longitude_code,
    /*elevation_min                  */ OLD.elevation_min,
    /*elevation_max                  */ OLD.elevation_max,
    /*latitude_deg_min               */ OLD.latitude_deg_min,
    /*latitude_min_min               */ OLD.latitude_min_min,
    /*latitude_sec_min               */ OLD.latitude_sec_min,
    /*latitude_deg_max               */ OLD.latitude_deg_max,
    /*latitude_min_max               */ OLD.latitude_min_max,
    /*latitude_sec_max               */ OLD.latitude_sec_max,
    /*longitude_deg_min              */ OLD.longitude_deg_min,
    /*longitude_min_min              */ OLD.longitude_min_min,
    /*longitude_sec_min              */ OLD.longitude_sec_min,
    /*longitude_deg_max              */ OLD.longitude_deg_max,
    /*longitude_min_max              */ OLD.longitude_min_max,
    /*longitude_sec_max              */ OLD.longitude_sec_max,
    /*smp_mean_bv_growth             */ OLD.smp_mean_bv_growth,
    /*area_of_use_comment            */ OLD.area_of_use_comment,
    /*approved_timestamp             */ OLD.approved_timestamp,
    /*approved_userid                */ OLD.approved_userid,
    /*mean_geom                      */ --OLD.mean_geom,

    /*update_userid                  */ OLD.update_userid,
    /*update_timestamp               */ OLD.update_timestamp,
    /*revision_count                 */ OLD.revision_count
    );
        RETURN OLD;
    elsif (TG_OP = 'INSERT') then
        v_new_data := ROW(NEW.*);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'I',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ 1, -- 1st row version
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,

	  /*seed_plan_unit_id              */ NEW.seed_plan_unit_id,
    /*bgc_zone_code                  */ NEW.bgc_zone_code,
    /*bgc_subzone_code               */ NEW.bgc_subzone_code,
    /*variant                        */ NEW.variant,
    /*bec_version_id                 */ NEW.bec_version_id,
    /*elevation                      */ NEW.elevation,
    /*latitude_degrees               */ NEW.latitude_degrees,
    /*latitude_minutes               */ NEW.latitude_minutes,
    /*latitude_seconds               */ NEW.latitude_seconds,
    /*longitude_degrees              */ NEW.longitude_degrees,
    /*longitude_minutes              */ NEW.longitude_minutes,
    /*longitude_seconds              */ NEW.longitude_seconds,
    /*collection_elevation           */ NEW.collection_elevation,
    /*collection_elevation_min       */ NEW.collection_elevation_min,
    /*collection_elevation_max       */ NEW.collection_elevation_max,
    /*collection_latitude_deg        */ NEW.collection_latitude_deg,
    /*collection_latitude_min        */ NEW.collection_latitude_min,
    /*collection_latitude_sec        */ NEW.collection_latitude_sec,
    /*collection_latitude_code       */ NEW.collection_latitude_code,
    /*collection_longitude_deg       */ NEW.collection_longitude_deg,
    /*collection_longitude_min       */ NEW.collection_longitude_min,
    /*collection_longitude_sec       */ NEW.collection_longitude_sec,
    /*collection_longitude_code      */ NEW.collection_longitude_code,
    /*elevation_min                  */ NEW.elevation_min,
    /*elevation_max                  */ NEW.elevation_max,
    /*latitude_deg_min               */ NEW.latitude_deg_min,
    /*latitude_min_min               */ NEW.latitude_min_min,
    /*latitude_sec_min               */ NEW.latitude_sec_min,
    /*latitude_deg_max               */ NEW.latitude_deg_max,
    /*latitude_min_max               */ NEW.latitude_min_max,
    /*latitude_sec_max               */ NEW.latitude_sec_max,
    /*longitude_deg_min              */ NEW.longitude_deg_min,
    /*longitude_min_min              */ NEW.longitude_min_min,
    /*longitude_sec_min              */ NEW.longitude_sec_min,
    /*longitude_deg_max              */ NEW.longitude_deg_max,
    /*longitude_min_max              */ NEW.longitude_min_max,
    /*longitude_sec_max              */ NEW.longitude_sec_max,
    /*smp_mean_bv_growth             */ NEW.smp_mean_bv_growth,
    /*area_of_use_comment            */ NEW.area_of_use_comment,
    /*approved_timestamp             */ NEW.approved_timestamp,
    /*approved_userid                */ NEW.approved_userid,
    /*mean_geom                  */ --NEW.mean_geom,

    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    else
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
        RETURN NULL;
    end if;

EXCEPTION
    WHEN data_exception then
        --insert into spar.error_catch (erro) VALUES(CONCAT('DATA EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN unique_violation then
      --insert into spar.error_catch (erro) VALUES(CONCAT('UNIQUE EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN others then
        --insert into spar.error_catch (erro) VALUES(CONCAT(v_auditrevision, CONCAT('OTHER EXCEPTION ',SQLERRM)));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, spar;

/* Update trigger */

CREATE OR REPLACE FUNCTION spar.seedlot_if_modified_func() RETURNS trigger AS $body$
DECLARE
    v_old_data TEXT;
    v_new_data TEXT;
  v_auditrevision int;
BEGIN
    if (TG_OP = 'UPDATE') then
        v_old_data := ROW(OLD.*);
        v_new_data := ROW(NEW.*);
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,/*mean_geom,*/update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'U',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,
    /*seed_plan_unit_id              */ NEW.seed_plan_unit_id,
    /*bgc_zone_code                  */ NEW.bgc_zone_code,
    /*bgc_subzone_code               */ NEW.bgc_subzone_code,
    /*variant                        */ NEW.variant,
    /*bec_version_id                 */ NEW.bec_version_id,
    /*elevation                      */ NEW.elevation,
    /*latitude_degrees               */ NEW.latitude_degrees,
    /*latitude_minutes               */ NEW.latitude_minutes,
    /*latitude_seconds               */ NEW.latitude_seconds,
    /*longitude_degrees              */ NEW.longitude_degrees,
    /*longitude_minutes              */ NEW.longitude_minutes,
    /*longitude_seconds              */ NEW.longitude_seconds,
    /*collection_elevation           */ NEW.collection_elevation,
    /*collection_elevation_min       */ NEW.collection_elevation_min,
    /*collection_elevation_max       */ NEW.collection_elevation_max,
    /*collection_latitude_deg        */ NEW.collection_latitude_deg,
    /*collection_latitude_min        */ NEW.collection_latitude_min,
    /*collection_latitude_sec        */ NEW.collection_latitude_sec,
    /*collection_latitude_code       */ NEW.collection_latitude_code,
    /*collection_longitude_deg       */ NEW.collection_longitude_deg,
    /*collection_longitude_min       */ NEW.collection_longitude_min,
    /*collection_longitude_sec       */ NEW.collection_longitude_sec,
    /*collection_longitude_code      */ NEW.collection_longitude_code,
    /*elevation_min                  */ NEW.elevation_min,
    /*elevation_max                  */ NEW.elevation_max,
    /*latitude_deg_min               */ NEW.latitude_deg_min,
    /*latitude_min_min               */ NEW.latitude_min_min,
    /*latitude_sec_min               */ NEW.latitude_sec_min,
    /*latitude_deg_max               */ NEW.latitude_deg_max,
    /*latitude_min_max               */ NEW.latitude_min_max,
    /*latitude_sec_max               */ NEW.latitude_sec_max,
    /*longitude_deg_min              */ NEW.longitude_deg_min,
    /*longitude_min_min              */ NEW.longitude_min_min,
    /*longitude_sec_min              */ NEW.longitude_sec_min,
    /*longitude_deg_max              */ NEW.longitude_deg_max,
    /*longitude_min_max              */ NEW.longitude_min_max,
    /*longitude_sec_max              */ NEW.longitude_sec_max,
    /*smp_mean_bv_growth             */ NEW.smp_mean_bv_growth,
    /*area_of_use_comment            */ NEW.area_of_use_comment,
    /*approved_timestamp             */ NEW.approved_timestamp,
    /*approved_userid                */ NEW.approved_userid,
    /*mean_geom                      */-- NEW.mean_geom,
    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    elsif (TG_OP = 'DELETE') then
        v_old_data := ROW(OLD.*);
    /* insert into spar.logged_actions (schema_name,table_name,user_name,action,original_data,query)
        values (TG_TABLE_SCHEMA::TEXT,TG_TABLE_NAME::TEXT,session_user::TEXT,substring(TG_OP,1,1),v_old_data, current_query());*/
    /* AUDIT REVISION number used to order the statements executed in the row */
    v_auditrevision := (SELECT MAX(COALESCE(audit_revision_version,1))+1 FROM spar.seedlot_audit WHERE seedlot_number = NEW.seedlot_number) ;
    INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,/*mean_geom,*/update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'D',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ coalesce(v_auditrevision,1),
    /*seedlot_number                 */ OLD.seedlot_number,
    /*seedlot_status_code            */ OLD.seedlot_status_code,
    /*applicant_client_number        */ OLD.applicant_client_number,
    /*applicant_locn_code            */ OLD.applicant_locn_code,
    /*applicant_email_address        */ OLD.applicant_email_address,
    /*vegetation_code                */ OLD.vegetation_code,
    /*genetic_class_code             */ OLD.genetic_class_code,
    /*seedlot_source_code            */ OLD.seedlot_source_code,
    /*to_be_registrd_ind             */ OLD.to_be_registrd_ind,
    /*bc_source_ind                  */ OLD.bc_source_ind,
    /*collection_client_number       */ OLD.collection_client_number,
    /*collection_locn_code           */ OLD.collection_locn_code,
    /*collection_start_date          */ OLD.collection_start_date,
    /*collection_end_date            */ OLD.collection_end_date,
    /*no_of_containers               */ OLD.no_of_containers,
    /*vol_per_container              */ OLD.vol_per_container,
    /*clctn_volume                   */ OLD.clctn_volume,
    /*seedlot_comment                */ OLD.seedlot_comment,
    /*interm_strg_client_number      */ OLD.interm_strg_client_number,
    /*interm_strg_locn_code          */ OLD.interm_strg_locn_code,
    /*interm_strg_st_date            */ OLD.interm_strg_st_date,
    /*interm_strg_end_date           */ OLD.interm_strg_end_date,
    /*interm_facility_code           */ OLD.interm_facility_code,
    /*female_gametic_mthd_code       */ OLD.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ OLD.male_gametic_mthd_code,
    /*controlled_cross_ind           */ OLD.controlled_cross_ind,
    /*biotech_processes_ind          */ OLD.biotech_processes_ind,
    /*pollen_contamination_ind       */ OLD.pollen_contamination_ind,
    /*pollen_contamination_pct       */ OLD.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ OLD.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ OLD.pollen_contamination_mthd_code,
    /*total_parent_trees             */ OLD.total_parent_trees,
    /*smp_success_pct                */ OLD.smp_success_pct,
    /*effective_pop_size             */ OLD.effective_pop_size,
    /*smp_parents_outside            */ OLD.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ OLD.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ OLD.extractory_client_number,
    /*extractory_locn_code           */ OLD.extractory_locn_code,
    /*extraction_st_date             */ OLD.extraction_st_date,
    /*extraction_end_date            */ OLD.extraction_end_date,
    /*temporary_strg_client_number   */ OLD.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ OLD.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ OLD.temporary_strg_start_date,
    /*temporary_strg_end_date        */ OLD.temporary_strg_end_date,
    /*interm_strg_locn               */ OLD.interm_strg_locn,
    /*declared_userid                */ OLD.declared_userid,
    /*declared_timestamp             */ OLD.declared_timestamp,
    /*entry_userid                   */ OLD.entry_userid,
    /*entry_timestamp                */ OLD.entry_timestamp,
	  /*seed_plan_unit_id              */ OLD.seed_plan_unit_id,
    /*bgc_zone_code                  */ OLD.bgc_zone_code,
    /*bgc_subzone_code               */ OLD.bgc_subzone_code,
    /*variant                        */ OLD.variant,
    /*bec_version_id                 */ OLD.bec_version_id,
    /*elevation                      */ OLD.elevation,
    /*latitude_degrees               */ OLD.latitude_degrees,
    /*latitude_minutes               */ OLD.latitude_minutes,
    /*latitude_seconds               */ OLD.latitude_seconds,
    /*longitude_degrees              */ OLD.longitude_degrees,
    /*longitude_minutes              */ OLD.longitude_minutes,
    /*longitude_seconds              */ OLD.longitude_seconds,
    /*collection_elevation           */ OLD.collection_elevation,
    /*collection_elevation_min       */ OLD.collection_elevation_min,
    /*collection_elevation_max       */ OLD.collection_elevation_max,
    /*collection_latitude_deg        */ OLD.collection_latitude_deg,
    /*collection_latitude_min        */ OLD.collection_latitude_min,
    /*collection_latitude_sec        */ OLD.collection_latitude_sec,
    /*collection_latitude_code       */ OLD.collection_latitude_code,
    /*collection_longitude_deg       */ OLD.collection_longitude_deg,
    /*collection_longitude_min       */ OLD.collection_longitude_min,
    /*collection_longitude_sec       */ OLD.collection_longitude_sec,
    /*collection_longitude_code      */ OLD.collection_longitude_code,
    /*elevation_min                  */ OLD.elevation_min,
    /*elevation_max                  */ OLD.elevation_max,
    /*latitude_deg_min               */ OLD.latitude_deg_min,
    /*latitude_min_min               */ OLD.latitude_min_min,
    /*latitude_sec_min               */ OLD.latitude_sec_min,
    /*latitude_deg_max               */ OLD.latitude_deg_max,
    /*latitude_min_max               */ OLD.latitude_min_max,
    /*latitude_sec_max               */ OLD.latitude_sec_max,
    /*longitude_deg_min              */ OLD.longitude_deg_min,
    /*longitude_min_min              */ OLD.longitude_min_min,
    /*longitude_sec_min              */ OLD.longitude_sec_min,
    /*longitude_deg_max              */ OLD.longitude_deg_max,
    /*longitude_min_max              */ OLD.longitude_min_max,
    /*longitude_sec_max              */ OLD.longitude_sec_max,
    /*smp_mean_bv_growth             */ OLD.smp_mean_bv_growth,
    /*area_of_use_comment            */ OLD.area_of_use_comment,
    /*approved_timestamp             */ OLD.approved_timestamp,
    /*approved_userid                */ OLD.approved_userid,
    /*mean_geom                      */ --OLD.mean_geom,
    /*update_userid                  */ OLD.update_userid,
    /*update_timestamp               */ OLD.update_timestamp,
    /*revision_count                 */ OLD.revision_count
    );
        RETURN OLD;
    elsif (TG_OP = 'INSERT') then
        v_new_data := ROW(NEW.*);
        INSERT INTO spar.seedlot_audit (spar_audit_code,db_user,audit_revision_version,seedlot_number,seedlot_status_code,applicant_client_number,applicant_locn_code,applicant_email_address,vegetation_code,genetic_class_code,seedlot_source_code,to_be_registrd_ind,bc_source_ind,collection_client_number,collection_locn_code,collection_start_date,collection_end_date,no_of_containers,vol_per_container,clctn_volume,seedlot_comment,interm_strg_client_number,interm_strg_locn_code,interm_strg_st_date,interm_strg_end_date,interm_facility_code,female_gametic_mthd_code,male_gametic_mthd_code,controlled_cross_ind,biotech_processes_ind,pollen_contamination_ind,pollen_contamination_pct,contaminant_pollen_bv,pollen_contamination_mthd_code,total_parent_trees,smp_success_pct,effective_pop_size,smp_parents_outside,non_orchard_pollen_contam_pct,extractory_client_number,extractory_locn_code,extraction_st_date,extraction_end_date,temporary_strg_client_number,temporary_strg_locn_code,temporary_strg_start_date,temporary_strg_end_date,interm_strg_locn,declared_userid,declared_timestamp,entry_userid,entry_timestamp,seed_plan_unit_id,bgc_zone_code,bgc_subzone_code,variant,bec_version_id,elevation,latitude_degrees,latitude_minutes,latitude_seconds,longitude_degrees,longitude_minutes,longitude_seconds,collection_elevation,collection_elevation_min,collection_elevation_max,collection_latitude_deg,collection_latitude_min,collection_latitude_sec,collection_latitude_code,collection_longitude_deg,collection_longitude_min,collection_longitude_sec,collection_longitude_code,elevation_min,elevation_max,latitude_deg_min,latitude_min_min,latitude_sec_min,latitude_deg_max,latitude_min_max,latitude_sec_max,longitude_deg_min,longitude_min_min,longitude_sec_min,longitude_deg_max,longitude_min_max,longitude_sec_max,smp_mean_bv_growth,area_of_use_comment,approved_timestamp,approved_userid,mean_geom,update_userid,update_timestamp,revision_count)
    VALUES(
    /*spar_audit_code                */ 'I',
    /*db_user                 	     */ session_user::TEXT,
    /*audit_revision_version  	     */ 1, -- 1st row version
    /*seedlot_number                 */ NEW.seedlot_number,
    /*seedlot_status_code            */ NEW.seedlot_status_code,
    /*applicant_client_number        */ NEW.applicant_client_number,
    /*applicant_locn_code            */ NEW.applicant_locn_code,
    /*applicant_email_address        */ NEW.applicant_email_address,
    /*vegetation_code                */ NEW.vegetation_code,
    /*genetic_class_code             */ NEW.genetic_class_code,
    /*seedlot_source_code            */ NEW.seedlot_source_code,
    /*to_be_registrd_ind             */ NEW.to_be_registrd_ind,
    /*bc_source_ind                  */ NEW.bc_source_ind,
    /*collection_client_number       */ NEW.collection_client_number,
    /*collection_locn_code           */ NEW.collection_locn_code,
    /*collection_start_date          */ NEW.collection_start_date,
    /*collection_end_date            */ NEW.collection_end_date,
    /*no_of_containers               */ NEW.no_of_containers,
    /*vol_per_container              */ NEW.vol_per_container,
    /*clctn_volume                   */ NEW.clctn_volume,
    /*seedlot_comment                */ NEW.seedlot_comment,
    /*interm_strg_client_number      */ NEW.interm_strg_client_number,
    /*interm_strg_locn_code          */ NEW.interm_strg_locn_code,
    /*interm_strg_st_date            */ NEW.interm_strg_st_date,
    /*interm_strg_end_date           */ NEW.interm_strg_end_date,
    /*interm_facility_code           */ NEW.interm_facility_code,
    /*female_gametic_mthd_code       */ NEW.female_gametic_mthd_code,
    /*male_gametic_mthd_code         */ NEW.male_gametic_mthd_code,
    /*controlled_cross_ind           */ NEW.controlled_cross_ind,
    /*biotech_processes_ind          */ NEW.biotech_processes_ind,
    /*pollen_contamination_ind       */ NEW.pollen_contamination_ind,
    /*pollen_contamination_pct       */ NEW.pollen_contamination_pct,
    /*contaminant_pollen_bv          */ NEW.contaminant_pollen_bv,
    /*pollen_contamination_mthd_code */ NEW.pollen_contamination_mthd_code,
    /*total_parent_trees             */ NEW.total_parent_trees,
    /*smp_success_pct                */ NEW.smp_success_pct,
    /*effective_pop_size             */ NEW.effective_pop_size,
    /*smp_parents_outside            */ NEW.smp_parents_outside,
    /*non_orchard_pollen_contam_pct  */ NEW.non_orchard_pollen_contam_pct,
    /*extractory_client_number       */ NEW.extractory_client_number,
    /*extractory_locn_code           */ NEW.extractory_locn_code,
    /*extraction_st_date             */ NEW.extraction_st_date,
    /*extraction_end_date            */ NEW.extraction_end_date,
    /*temporary_strg_client_number   */ NEW.temporary_strg_client_number,
    /*temporary_strg_locn_code       */ NEW.temporary_strg_locn_code,
    /*temporary_strg_start_date      */ NEW.temporary_strg_start_date,
    /*temporary_strg_end_date        */ NEW.temporary_strg_end_date,
    /*interm_strg_locn               */ NEW.interm_strg_locn,
    /*declared_userid                */ NEW.declared_userid,
    /*declared_timestamp             */ NEW.declared_timestamp,
    /*entry_userid                   */ NEW.entry_userid,
    /*entry_timestamp                */ NEW.entry_timestamp,
	  /*seed_plan_unit_id              */ NEW.seed_plan_unit_id,
    /*bgc_zone_code                  */ NEW.bgc_zone_code,
    /*bgc_subzone_code               */ NEW.bgc_subzone_code,
    /*variant                        */ NEW.variant,
    /*bec_version_id                 */ NEW.bec_version_id,
    /*elevation                      */ NEW.elevation,
    /*latitude_degrees               */ NEW.latitude_degrees,
    /*latitude_minutes               */ NEW.latitude_minutes,
    /*latitude_seconds               */ NEW.latitude_seconds,
    /*longitude_degrees              */ NEW.longitude_degrees,
    /*longitude_minutes              */ NEW.longitude_minutes,
    /*longitude_seconds              */ NEW.longitude_seconds,
    /*collection_elevation           */ NEW.collection_elevation,
    /*collection_elevation_min       */ NEW.collection_elevation_min,
    /*collection_elevation_max       */ NEW.collection_elevation_max,
    /*collection_latitude_deg        */ NEW.collection_latitude_deg,
    /*collection_latitude_min        */ NEW.collection_latitude_min,
    /*collection_latitude_sec        */ NEW.collection_latitude_sec,
    /*collection_latitude_code       */ NEW.collection_latitude_code,
    /*collection_longitude_deg       */ NEW.collection_longitude_deg,
    /*collection_longitude_min       */ NEW.collection_longitude_min,
    /*collection_longitude_sec       */ NEW.collection_longitude_sec,
    /*collection_longitude_code      */ NEW.collection_longitude_code,
    /*elevation_min                  */ NEW.elevation_min,
    /*elevation_max                  */ NEW.elevation_max,
    /*latitude_deg_min               */ NEW.latitude_deg_min,
    /*latitude_min_min               */ NEW.latitude_min_min,
    /*latitude_sec_min               */ NEW.latitude_sec_min,
    /*latitude_deg_max               */ NEW.latitude_deg_max,
    /*latitude_min_max               */ NEW.latitude_min_max,
    /*latitude_sec_max               */ NEW.latitude_sec_max,
    /*longitude_deg_min              */ NEW.longitude_deg_min,
    /*longitude_min_min              */ NEW.longitude_min_min,
    /*longitude_sec_min              */ NEW.longitude_sec_min,
    /*longitude_deg_max              */ NEW.longitude_deg_max,
    /*longitude_min_max              */ NEW.longitude_min_max,
    /*longitude_sec_max              */ NEW.longitude_sec_max,
    /*smp_mean_bv_growth             */ NEW.smp_mean_bv_growth,
    /*area_of_use_comment            */ NEW.area_of_use_comment,
    /*approved_timestamp             */ NEW.approved_timestamp,
    /*approved_userid                */ NEW.approved_userid,
    /*mean_geom                      */ NEW.mean_geom,
    /*update_userid                  */ NEW.update_userid,
    /*update_timestamp               */ NEW.update_timestamp,
    /*revision_count                 */ NEW.revision_count
    );
        RETURN NEW;
    else
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - Other action occurred: %, at %',TG_OP,now();
        RETURN NULL;
    end if;

EXCEPTION
    WHEN data_exception then
        --insert into spar.error_catch (erro) VALUES(CONCAT('DATA EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [DATA EXCEPTION] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN unique_violation then
      --insert into spar.error_catch (erro) VALUES(CONCAT('UNIQUE EXCEPTION ',SQLERRM));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [UNIQUE] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
    WHEN others then
        --insert into spar.error_catch (erro) VALUES(CONCAT(v_auditrevision, CONCAT('OTHER EXCEPTION ',SQLERRM)));
        RAISE WARNING '[AUDIT.IF_MODIFIED_FUNC] - UDF ERROR [OTHER] - SQLSTATE: %, SQLERRM: %',SQLSTATE,SQLERRM;
        RETURN NULL;
END;
$body$
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, spar;

-- Drop existing trigger
DROP TRIGGER trg_seedlot_audit_DIU ON spar.seedlot;

-- Trigger to be attached on spar.seedlot table
CREATE TRIGGER trg_seedlot_audit_DIU
 AFTER INSERT OR UPDATE OR DELETE ON spar.seedlot
 FOR EACH ROW EXECUTE PROCEDURE spar.seedlot_if_modified_func();


ALTER TABLE
  spar.seedlot_seed_plan_zone
ADD
  COLUMN primary_ind boolean default false,
ADD
  COLUMN seed_plan_zone_description varchar(120),
ALTER
  COLUMN entry_timestamp DROP NOT NULL,
ALTER
  COLUMN update_timestamp DROP NOT NULL;


CREATE OR REPLACE FUNCTION ensure_single_primary_ind()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.primary_ind THEN
        IF EXISTS (
            SELECT 1
            FROM spar.seedlot_seed_plan_zone AS sspz
            WHERE sspz.seedlot_number = NEW.seedlot_number
            AND sspz.primary_ind = true
            AND sspz.seed_plan_zone_code <> NEW.seed_plan_zone_code
        ) THEN
            RAISE EXCEPTION 'Only one row per seedlot_number can have primary_ind = true';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ensure_single_primary_ind_trigger
BEFORE INSERT OR UPDATE ON spar.seedlot_seed_plan_zone
FOR EACH ROW EXECUTE FUNCTION ensure_single_primary_ind();


-- Step 1: Add the 'primary_ind' column
ALTER TABLE spar.seedlot_orchard
ADD COLUMN primary_ind boolean NOT NULL DEFAULT false;

-- Step 2: Update 'primary_ind' for existing rows based on the least recent update_timestamp
UPDATE spar.seedlot_orchard AS s1
SET primary_ind = TRUE
FROM (
    SELECT seedlot_number, MIN(update_timestamp) AS min_update_time
    FROM spar.seedlot_orchard
    GROUP BY seedlot_number
) AS s2
WHERE s1.seedlot_number = s2.seedlot_number
AND s1.update_timestamp = s2.min_update_time;

-- Step 3: Change the primary key to (seedlot_number, primary_ind)
ALTER TABLE spar.seedlot_orchard
DROP CONSTRAINT IF EXISTS seedlot_orchard_pk,
ADD PRIMARY KEY (seedlot_number, primary_ind);

-- Step 4: Create a unique key constraint for (seedlot_number, orchard_id)
ALTER TABLE spar.seedlot_orchard
ADD CONSTRAINT unique_seedlot_orchard_key UNIQUE (seedlot_number, orchard_id);







/*
----- ETL Tool Changes after PoC (LATEST VERSION=V_36)
*/

/* ---------------------------
DDL
------------------------------   */

DROP TABLE IF EXISTS spar.ETL_EXECUTION_MAP;
DROP TABLE IF EXISTS spar.ETL_EXECUTION_LOG;
DROP TABLE IF EXISTS spar.ETL_EXECUTION_LOG_HIST;
DROP TABLE IF EXISTS spar.ETL_EXECUTION_SCHEDULE;


create table if not exists spar.ETL_EXECUTION_MAP(
interface_id 		  varchar(100) not null,
execution_id 		  integer not null,
execution_parent_id   integer,
execution_order 	  integer,
source_file 		  varchar(200),
source_db_type 		  varchar(200),
source_name 		  varchar(100),
source_table 		  varchar(100),
target_file 		  varchar(200),
target_db_type 		  varchar(200),
target_name 		  varchar(100),
target_table 		  varchar(100),
target_primary_key 	  varchar(100),
run_mode          varchar(25)     default 'UPSERT' not null
   check(run_mode in ('DELETE_INSERT','UPSERT','UPSERT_WTIH_DELETE')),
upsert_with_delete_key    varchar(100),
ignore_columns_on_update  varchar(200),
retry_errors   		  boolean     default false not null,
updated_at  		  timestamp   default now() not null,
created_at  		  timestamp   default now() not null,
constraint etl_execution_map_pk
    primary key (interface_id,execution_id)
);

comment on table spar.ETL_EXECUTION_MAP is 'ETL Tool monitoring table to store execution details of batch processing interfaces';
comment on column spar.ETL_EXECUTION_MAP.interface_id               is 'Unique interface name to represent a batch execution. Part of the composite PK, with execution_id column.';
comment on column spar.ETL_EXECUTION_MAP.execution_id               is 'Execution ID number to represent a instance of interface_id. Part of the composite PK, with interface_id column.';
comment on column spar.ETL_EXECUTION_MAP.execution_parent_id        is 'Reference to a parent execution ID that groups one or more execution_id in a batch run. If null, then this execution_id is a parent.';
comment on column spar.ETL_EXECUTION_MAP.execution_order            is 'Order of execution of this instance in a collection of same execution_parent_id. Execution runs in ascending order. If it is less than 0, it will not run.';
comment on column spar.ETL_EXECUTION_MAP.source_file                is 'Source instruction file for batch execution';
comment on column spar.ETL_EXECUTION_MAP.source_db_type             is 'Database type of the source connection. Expected ORACLE or POSTGRES.';
comment on column spar.ETL_EXECUTION_MAP.source_name                is 'Source name of this batch execution';
comment on column spar.ETL_EXECUTION_MAP.source_table               is 'Source table (if it is a single table) of this batch execution';
comment on column spar.ETL_EXECUTION_MAP.target_file                is 'Target instruction file for batch execution';
comment on column spar.ETL_EXECUTION_MAP.target_db_type             is 'Database type of the target connection. Expected ORACLE or POSTGRES.';
comment on column spar.ETL_EXECUTION_MAP.target_name                is 'Target name of this batch execution';
comment on column spar.ETL_EXECUTION_MAP.target_table               is 'Target table (if it is a single table) of this batch execution';
comment on column spar.ETL_EXECUTION_MAP.target_primary_key         is 'Primary key of the target table of this batch execution';
comment on column spar.ETL_EXECUTION_MAP.run_mode                   is 'Identifies how data is replicated:DELETE_INSERT,UPSERT,UPSERT_WTIH_DELETE';
comment on column spar.ETL_EXECUTION_MAP.upsert_with_delete_key     is 'For run_mode UPSERT_WTIH_DELETE, specifies the WHERE clause columns for delete';
comment on column spar.ETL_EXECUTION_MAP.ignore_columns_on_update   is 'Comma seperated list of columns to ignore when updating';
comment on column spar.ETL_EXECUTION_MAP.retry_errors        		    is 'If true, this process will execute again old instances with errors in ETL_EXECUTION_LOG_HIST';
comment on column spar.ETL_EXECUTION_MAP.updated_at                 is 'Timestamp of the last time this record was updated';
comment on column spar.ETL_EXECUTION_MAP.created_at                 is 'Timestamp of the time this record was created';

create table spar.ETL_EXECUTION_LOG(
from_timestamp timestamp not null,
to_timestamp   timestamp not null,
run_status varchar(100) not null,
updated_at  timestamp   default now() not null,
created_at  timestamp   default now() not null
);


comment on table spar.ETL_EXECUTION_LOG is 'ETL Tool monitoring table to store execution current instance of batch processing interfaces';
comment on column spar.ETL_EXECUTION_LOG.from_timestamp             is 'From timestamp for the run (i.e. update_timestamp between from_timestamp and to_timetsamp)';
comment on column spar.ETL_EXECUTION_LOG.to_timestamp               is 'To timestamp for the run (i.e. update_timestamp between from_timestamp and to_timetsamp)';
comment on column spar.ETL_EXECUTION_LOG.run_status                 is 'Status of ETL execution';
comment on column spar.ETL_EXECUTION_LOG.updated_at                 is 'Timestamp of the last time this record was updated';
comment on column spar.ETL_EXECUTION_LOG.created_at                 is 'Timestamp of the time this record was created';


create table spar.ETL_EXECUTION_SCHEDULE(
interface_id 	varchar(100) not null,
execution_id 	integer      not null,
last_run_ts		timestamp,
current_run_ts  timestamp,
updated_at      timestamp    default now() not null,
created_at  	timestamp    default now() not null,
constraint etl_execution_schedule_pk
	primary key (interface_id,execution_id)
);

comment on table  spar.ETL_EXECUTION_SCHEDULE 				is 'ETL Tool schedule table to define what delta time should be used in the next execution';
comment on column spar.ETL_EXECUTION_SCHEDULE.interface_id       is 'Unique interface name to represent a batch execution. Refer to EXECUTION_MAP table PK';
comment on column spar.ETL_EXECUTION_SCHEDULE.execution_id       is 'The execution ID that represent a batch execution. Refer to EXECUTION_MAP table PK';
comment on column spar.ETL_EXECUTION_SCHEDULE.last_run_ts        is 'Last timestamp this interface/execution_id was executed for batch execution';
comment on column spar.ETL_EXECUTION_SCHEDULE.current_run_ts     is 'Current timestamp this interface was executed of this batch execution. It will be the next last_run_ts for this execution.';
comment on column spar.ETL_EXECUTION_SCHEDULE.updated_at         is 'Timestamp of the last time this record was updated';
comment on column spar.ETL_EXECUTION_SCHEDULE.created_at         is 'Timestamp of the time this record was created';


create table spar.etl_execution_log_hist
( entry_timestamp timestamp(6) not null default current_timestamp
, log_details jsonb not null);

comment on table  spar.ETL_EXECUTION_LOG_HIST is 'ETL Tool monitoring table to store all executed instances of batch processing interfaces';
comment on column spar.ETL_EXECUTION_LOG_HIST.entry_timestamp      		  is 'The timestamp when the record was inserted';
comment on column spar.ETL_EXECUTION_LOG_HIST.log_details       		    is 'JSON document with step statistics';

/*
-- DML for Generic interface_id for generic running
*/

/* ORACLE TO POSTGRES ORCHESTRATION: TEST EXECUTION */
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 0 				as execution_id,
       null 			as execution_parent_id ,
       'ETL-RUN-ORACLE-TO-POSTGRES-TEST' 		as interface_id,
       null 			as source_file,
       'MAIN PROCESS FROM ORACLE' 	as source_name,
       null 			as source_table,
       null 			as source_db_type,
       null 			as target_file,
       'MAIN PROCESS TO POSTGRES' 	as target_name,
       null 			as target_table,
       null 			as target_db_type,
       null 			as target_primary_key,
	   0 				as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'ETL-RUN-ORACLE-TO-POSTGRES-TEST');

insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 1 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SEEDLOT_EXTRACT.sql'   as source_file,
       'ORACLE THE'                 			as source_name,
       'SEEDLOT'               	    			as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SEEDLOT_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot' 							as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number' 						as target_primary_key,
	   1 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-ORACLE-TO-POSTGRES-TEST');

/* POSTGRES TO ORACLE ORCHESTRATION: TEST EXECUTION */
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 2 				as execution_id,
       null 			as execution_parent_id ,
       'ETL-RUN-POSTGRES-TO-ORACLE-TEST' 		as interface_id,
       null 			as source_file,
       'MAIN PROCESS FROM `POSTGRES' 	    as source_name,
       null 			as source_table,
       null 			as source_db_type,
       null 			as target_file,
       'MAIN PROCESS TO ORACLE' 	        as target_name,
       null 			as target_table,
       null 			as target_db_type,
       null 			as target_primary_key,
	   0 				as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'ETL-RUN-POSTGRES-TO-ORACLE-TEST');

insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 3 										as execution_id,
       2 										as execution_parent_id ,
       'SEEDLOT-ORACLE-TO-SPAR-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/POSTGRES_SEEDLOT_EXTRACT.sql' as source_file,
       'NEW SPAR'                    			as source_name,
       'spar.seedlot'          	    			as source_table,
       'POSTGRES'          	    				as source_db_type,
       '/SQL/SPAR/ORACLE_SEEDLOT_LOAD.sql' 	    as target_file,
       'ORACLE THE' 							as target_name,
       'SEEDLOT' 							    as target_table,
       'ORACLE' 							    as target_db_type,
       'seedlot_number' 						as target_primary_key,
	   1 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SEEDLOT-ORACLE-TO-SPAR-POSTGRES-TEST');

---- MAIN EXECUTION
-- Processes gathering data from Oracle to Postgres (First to bring all historical data)

-- INCLUDING PROCESS SMP_MIX from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 4 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SMPMIX-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SMP_MIX_EXTRACT.sql'   as source_file,
       'ORACLE THE'                 			as source_name,
       'SMP_MIX'               	    			as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SMP_MIX_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.smp_mix' 							as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,parent_tree_id' 			as target_primary_key,
	   2 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SMPMIX-ORACLE-TO-POSTGRES-TEST');

insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 5 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SMPMIX-GEN-QLTY-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SMP_MIX_GEN_QLTY_EXTRACT.sql'   as source_file,
       'ORACLE THE'                 			as source_name,
       'SMP_MIX_GEN_QLTY'               	    			as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SMP_MIX_GEN_QLTY_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.smp_mix_gen_qlty' 							as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,parent_tree_id,genetic_type_code,genetic_worth_code' 					as target_primary_key,
	   3 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SMPMIX-GEN-QLTY-ORACLE-TO-POSTGRES-TEST');


-- INCLUDING PROCESS SEEDLOT_PARENT_TREE_SMP_MIX from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 6 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-PARENT-TREE-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SEEDLOT_PARENT_TREE_EXTRACT.sql'   as source_file,
       'ORACLE THE'                 			as source_name,
       'SEEDLOT_PARENT_TREE'               	    as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SEEDLOT_PARENT_TREE_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot_parent_tree' 				as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,parent_tree_id'          as target_primary_key,
	   4 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-PARENT-TREE-ORACLE-TO-POSTGRES-TEST');

-- INCLUDING PROCESS SEEDLOT_PARENT_TREE_SMP_MIX from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 7 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-PARENT-TREE-SMPMIX-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_PARENT_TREE_SMPMIX_EXTRACT.sql'   as source_file,
       'ORACLE THE'                 			as source_name,
       'SMP_MIX_GEN_QLTY'               	   	as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_PARENT_TREE_SMPMIX_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot_parent_tree_smp_mix' 							as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,parent_tree_id,genetic_type_code,genetic_worth_code' 					as target_primary_key,
	   5 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-PARENT-TREE-SMPMIX-ORACLE-TO-POSTGRES-TEST');

-- INCLUDING PROCESS SEEDLOT_ORCHARD from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 8 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-ORCHARD-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SEEDLOT_ORCHARD_EXTRACT.sql'   as source_file, -- SAME AS SMP_MIX_GEN_QLTY Extract (same structure)
       'ORACLE THE'                 			as source_name,
       'SEEDLOT'               	    			as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SEEDLOT_ORCHARD_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot_orchard' 					as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,primary_ind' 			as target_primary_key,
	   6 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-ORCHARD-ORACLE-TO-POSTGRES-TEST');

-- INCLUDING PROCESS SEEDLOT_GENETIC_WORTH from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 9 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-GENETIC-WORTH-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SEEDLOT_GENETIC_WORTH_EXTRACT.sql'   as source_file,
       'ORACLE THE'                 			as source_name,
       'SMP_MIX_GEN_QLTY'           			as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SEEDLOT_GENETIC_WORTH_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot_genetic_worth' 			as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,genetic_worth_code' 		as target_primary_key,
	   7 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-GENETIC-WORTH-ORACLE-TO-POSTGRES-TEST');



-- INCLUDING PROCESS SEEDLOT_OWNER_QUANTITY from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 10 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-OWNER-QUANTITY-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SEEDLOT_OWNER_QUANTITY_EXTRACT.sql'   as source_file, -- SAME AS SMP_MIX_GEN_QLTY Extract (same structure)
       'ORACLE THE'                 			as source_name,
       'SEEDLOT_OWNER_QUANTITY'           			as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SEEDLOT_OWNER_QUANTITY_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot_owner_quantity' 			as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,owner_client_number,owner_locn_code' 		as target_primary_key,
	   8 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-OWNER-QUANTITY-ORACLE-TO-POSTGRES-TEST');




-- INCLUDING PROCESS seedlot_collection_method from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 11 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-COLLECTION-METHOD-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SEEDLOT_COLLECTION_METHOD_EXTRACT.sql'   as source_file, -- SAME AS SMP_MIX_GEN_QLTY Extract (same structure)
       'ORACLE THE'                 			as source_name,
       'CONE_COLLECTION_METHOD_CODE'       		as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SEEDLOT_COLLECTION_METHOD_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot_collection_method' 		as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,cone_collection_method_code' 		as target_primary_key,
	   9 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-COLLECTION-METHOD-ORACLE-TO-POSTGRES-TEST');


-- INCLUDING PROCESS SEEDLOT_PARENT_TREE_GEN_QLTY from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 12 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-PARENT-TREE-GEN-QLTY-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SEEDLOT_PARENT_TREE_GEN_QLTY_EXTRACT.sql'   as source_file, -- SAME AS SMP_MIX_GEN_QLTY Extract (same structure)
       'ORACLE THE'                 			as source_name,
       'SEEDLOT_PARENT_TREE_GEN_QLTY'       		as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SEEDLOT_COLLECTION_METHOD_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot_parent_tree_gen_qlty' 		as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,parent_tree_id,genetic_type_code,genetic_worth_code' 	as target_primary_key,
	   10 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-PARENT-TREE-GEN-QLTY-ORACLE-TO-POSTGRES-TEST');


-- INCLUDING PROCESS SEEDLOT_SEED_PLAN_ZONE from Oracle to Postgres in EXECUTION_ID=0
insert into spar.etl_execution_map(execution_id, execution_parent_id ,interface_id, source_file,source_name, source_table, source_db_type,
                                   target_file,target_name, target_table, target_db_type,target_primary_key, execution_order)
select 13 										as execution_id,
       0 										as execution_parent_id ,
       'SPAR-SEEDLOT-SEED-PLAN-ZONE-ORACLE-TO-POSTGRES-TEST' 	    as interface_id,
       '/SQL/SPAR/ORACLE_SEEDLOT_SEED_PLAN_ZONE_EXTRACT.sql'   as source_file, -- SAME AS SMP_MIX_GEN_QLTY Extract (same structure)
       'ORACLE THE'                 			as source_name,
       'SEEDLOT_PLAN_ZONE'       				as source_table,
       'ORACLE'               	    			as source_db_type,
       '/SQL/SPAR/POSTGRES_SEEDLOT_SEED_PLAN_ZONE_UPSERT.sql' 	as target_file,
       'NEW SPAR' 								as target_name,
       'spar.seedlot_seed_plan_zone' 			as target_table,
       'POSTGRES' 								as target_db_type,
       'seedlot_number,seed_plan_zone_code' 	as target_primary_key,
	   11 										as execution_order
where not exists (select 1 from spar.etl_execution_map where interface_id = 'SPAR-SEEDLOT-SEED-PLAN-ZONE-ORACLE-TO-POSTGRES-TEST');