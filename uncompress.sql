SELECT * FROM car_operation_evs_2018.bms_alarm_info where car_id=40;
SELECT count(*) FROM car_operation_evs_2018.bms_alarm_info where car_id=40;
select uncompress(cell_volt)  from bms_alarm_info limit 1;
select uncompressed_length(cell_volt) from bms_alarm_info;