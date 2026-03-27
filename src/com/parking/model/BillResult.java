package com.parking.model;

import java.time.LocalDateTime;

public class BillResult {

    public final String numberPlate;
    public final int floor;
    public final int slotId; // DB primary key — used for FK in parking_history
    public final int slotNum; // human-readable slot number
    public final String vehicleType;
    public final LocalDateTime entryTime;
    public final LocalDateTime exitTime;
    public final int allowedMins;
    public final long totalMins;
    public final long extraMins;
    public final double totalAmount;
    public final double penaltyAmount;

    public BillResult(String numberPlate,
            int floor,
            int slotId,
            int slotNum,
            String vehicleType,
            LocalDateTime entryTime,
            LocalDateTime exitTime,
            int allowedMins,
            long totalMins,
            long extraMins,
            double totalAmount,
            double penaltyAmount) {

        this.numberPlate = numberPlate;
        this.floor = floor;
        this.slotId = slotId;
        this.slotNum = slotNum;
        this.vehicleType = vehicleType;
        this.entryTime = entryTime;
        this.exitTime = exitTime;
        this.allowedMins = allowedMins;
        this.totalMins = totalMins;
        this.extraMins = extraMins;
        this.totalAmount = totalAmount;
        this.penaltyAmount = penaltyAmount;
    }

    @Override
    public String toString() {
        return "BillResult{" +
                "plate=" + numberPlate +
                ", floor=" + floor +
                ", slotId=" + slotId +
                ", slotNum=" + slotNum +
                ", type=" + vehicleType +
                ", entry=" + entryTime +
                ", exit=" + exitTime +
                ", allowedMins=" + allowedMins +
                ", totalMins=" + totalMins +
                ", extraMins=" + extraMins +
                ", total=" + totalAmount +
                ", penalty=" + penaltyAmount +
                "}";
    }
}