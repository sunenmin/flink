package com.sun.models;

import lombok.Data;

/**
 * @Author Matt Sun
 * @Date 2021/3/20 7:04 下午
 * @Version 1.0
 **/

@Data
public class BatteryData {
    private String vin;
    private Long timestamp;
    private Double totalVoltage;
    private Double totalCurrent;
    private Short vehicleChrgSts;

    public BatteryData() {
    }

    public BatteryData(String vin, Long timestamp, Double totalVoltage, Double totalCurrent, Short vehicleChrgSts) {
        this.vin = vin;
        this.timestamp = timestamp;
        this.totalVoltage = totalVoltage;
        this.totalCurrent = totalCurrent;
        this.vehicleChrgSts = vehicleChrgSts;
    }
}
