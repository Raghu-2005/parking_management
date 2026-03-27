package com.parking.model;

public class Slot {

    // ── EXISTING FIELDS (unchanged) ──────────────────────
    public final int id;
    public final int floor;
    public final int slotNum;
    public final String vehicleType;
    public final int allowedMins;
    public final double ratePerHr;
    public final double penaltyPerHr;

    // ── NEW FIELDS (slot partition / combine) ────────────
    // slotRow : logical slot-row index (0, 1, 2, 3…)
    // grid_row = slotRow × (1 + path_width)
    // colStart : leftmost column this slot occupies
    // colEnd : rightmost column (= colStart + size - 1)
    // size : number of grid units this slot occupies
    // bike=1, car=2, truck=4
    // -1 means not yet assigned to the grid
    public final int slotRow;
    public final int colStart;
    public final int colEnd;
    public final int size;

    // ── FULL CONSTRUCTOR (used by new DAO queries) ───────
    public Slot(int id,
            int floor,
            int slotNum,
            String vehicleType,
            int allowedMins,
            double ratePerHr,
            double penaltyPerHr,
            int slotRow,
            int colStart,
            int colEnd,
            int size) {

        this.id = id;
        this.floor = floor;
        this.slotNum = slotNum;
        this.vehicleType = vehicleType;
        this.allowedMins = allowedMins;
        this.ratePerHr = ratePerHr;
        this.penaltyPerHr = penaltyPerHr;
        this.slotRow = slotRow;
        this.colStart = colStart;
        this.colEnd = colEnd;
        this.size = size;
    }

    // ── LEGACY CONSTRUCTOR (keeps existing DAO calls compiling) ──
    // Used everywhere the new columns are not yet read
    // (history queries, availability queries, etc.).
    // Sets grid fields to -1 meaning "not assigned to grid".
    public Slot(int id,
            int floor,
            int slotNum,
            String vehicleType,
            int allowedMins,
            double ratePerHr,
            double penaltyPerHr) {

        this(id, floor, slotNum, vehicleType,
                allowedMins, ratePerHr, penaltyPerHr,
                -1, -1, -1, -1);
    }

    // ── HELPER ───────────────────────────────────────────
    // Returns true if this slot has been assigned a grid
    // position. Slots created before the migration, or slots
    // created via the old API without grid params, return false.
    public boolean hasGridPosition() {
        return slotRow >= 0 && colStart >= 0;
    }

    @Override
    public String toString() {
        return "Slot{"
                + "id=" + id
                + ", floor=" + floor
                + ", slotNum=" + slotNum
                + ", type=" + vehicleType
                + ", allowedMins=" + allowedMins
                + ", rate=" + ratePerHr
                + ", penalty=" + penaltyPerHr
                + ", slotRow=" + slotRow
                + ", colStart=" + colStart
                + ", colEnd=" + colEnd
                + ", size=" + size
                + "}";
    }
}