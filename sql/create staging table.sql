create table if not exists staging_vra.vra_raw_flights (
	airline_icao 			VARCHAR(255),
	airline 				VARCHAR(255),
	flight_number 			VARCHAR(255),
	di_code 				VARCHAR(255),
	line_type_code 			VARCHAR(255),
	aircraft_icao 			VARCHAR(255),
	passenger_count 		VARCHAR(255),
	orig_airport_icao 		VARCHAR(255),
	orig_airport 			VARCHAR(255),
	scheduled_departure		VARCHAR(255),
	actual_departure 		VARCHAR(255),
	dest_airport_icao 		VARCHAR(255),
	dest_airport 			VARCHAR(255),
	scheduled_arrival 		VARCHAR(255),
	actual_arrival 			VARCHAR(255),
	flight_status 			VARCHAR(255),
	justification 			VARCHAR(255),
	reference 				VARCHAR(255),
	departure_situation 	VARCHAR(255),
	arrival_situation 		VARCHAR(255)
)

