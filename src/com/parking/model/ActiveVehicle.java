package com.parking.model;

import java.time.LocalDateTime;

public class ActiveVehicle {

    public final String        numberPlate;
    public final int           slotId;
    public final int           floor;
    public final String        vehicleType;
    public final LocalDateTime entryTime;

    public ActiveVehicle(String        numberPlate,
                         int           slotId,
                         int           floor,
                         String        vehicleType,
                         LocalDateTime entryTime) {

        this.numberPlate = numberPlate;
        this.slotId      = slotId;
        this.floor       = floor;
        this.vehicleType = vehicleType;
        this.entryTime   = entryTime;
    }

    @Override
    public String toString() {
        return "ActiveVehicle{" +
               "plate="      + numberPlate +
               ", slotId="   + slotId      +
               ", floor="    + floor       +
               ", type="     + vehicleType +
               ", entry="    + entryTime   +
               "}";
    }
}
