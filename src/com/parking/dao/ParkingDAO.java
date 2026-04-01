package com.parking.dao;
import java.util.LinkedHashMap;

import com.parking.model.ActiveVehicle;
import com.parking.model.BillResult;
import com.parking.model.Slot;
import com.parking.pool.ConnectionPool;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ParkingDAO {

    private final ConnectionPool pool;

    // Stats cache: key = filter string, value = [stats map, expiry timestamp]
    // TTL is 60 seconds and shared across all admin sessions.
    private final ConcurrentHashMap<String, Object[]> statsCache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 60_000L;

    public ParkingDAO(ConnectionPool pool) {
        this.pool = pool;
    }

    // ── STARTUP LOADS ────────────────────────────────────

    // Loads all parking slots from DB on startup.
    // Nullable grid columns (slot_row, col_start, col_end, size) are defaulted
    // to -1 using COALESCE — hasGridPosition() can be used to check if a slot
    // is on the grid.
    public List<Slot> loadAllSlots() throws SQLException {
        String sql = "SELECT id, floor_num, slot_num, vehicle_type, "
                + "allowed_mins, rate_per_hr, penalty_per_hr, "
                + "COALESCE(slot_row,  -1) AS slot_row, "
                + "COALESCE(col_start, -1) AS col_start, "
                + "COALESCE(col_end,   -1) AS col_end, "
                + "COALESCE(size,      -1) AS size "
                + "FROM parking_slots";

        List<Slot> slots = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                slots.add(new Slot(
                        rs.getInt("id"),
                        rs.getInt("floor_num"),
                        rs.getInt("slot_num"),
                        rs.getString("vehicle_type"),
                        rs.getInt("allowed_mins"),
                        rs.getDouble("rate_per_hr"),
                        rs.getDouble("penalty_per_hr"),
                        rs.getInt("slot_row"),
                        rs.getInt("col_start"),
                        rs.getInt("col_end"),
                        rs.getInt("size")));
            }
        }
        return slots;
    }

    // Loads all currently active (parked) vehicles from DB on startup.
    public List<ActiveVehicle> loadActiveVehicles() throws SQLException {
        String sql = "SELECT number_plate, slot_id, floor_num, "
                + "vehicle_type, entry_time "
                + "FROM active_parking";

        List<ActiveVehicle> vehicles = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                vehicles.add(new ActiveVehicle(
                        rs.getString("number_plate"),
                        rs.getInt("slot_id"),
                        rs.getInt("floor_num"),
                        rs.getString("vehicle_type"),
                        rs.getTimestamp("entry_time").toLocalDateTime()));
            }
        }
        return vehicles;
    }

    // Loads all currently blocked floors and their reasons from DB on startup.
    public Map<Integer, String> loadBlockedFloors() throws SQLException {
        String sql = "SELECT floor_num, reason FROM blocked_floor";

        Map<Integer, String> blocked = new HashMap<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                blocked.put(
                        rs.getInt("floor_num"),
                        rs.getString("reason"));
            }
        }
        return blocked;
    }

    // ── ACTIVE VEHICLES — PAGED ───────────────────────────

    // Returns one page of active vehicles (page is 1-based, pageSize = 50).
    // slot_num is joined directly to avoid extra queries per row.
    public List<Map<String, Object>> getActiveVehiclesPaged(
            int page, int pageSize) throws SQLException {

        int offset = (page - 1) * pageSize;
        String sql = "SELECT ap.number_plate, ap.slot_id, ap.floor_num, "
                + "ap.vehicle_type, ap.entry_time, ps.slot_num "
                + "FROM active_parking ap "
                + "JOIN parking_slots ps ON ap.slot_id = ps.id "
                + "ORDER BY ap.entry_time DESC "
                + "LIMIT ? OFFSET ?";

        List<Map<String, Object>> list = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, pageSize);
            ps.setInt(2, offset);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("number_plate", rs.getString("number_plate"));
                    row.put("slot_id", rs.getInt("slot_id"));
                    row.put("slot_num", rs.getInt("slot_num"));
                    row.put("floor_num", rs.getInt("floor_num"));
                    row.put("vehicle_type", rs.getString("vehicle_type"));
                    row.put("entry_time", rs.getTimestamp("entry_time")
                            .toLocalDateTime().toString().replace("T", " "));
                    list.add(row);
                }
            }
        }
        return list;
    }

    // Returns total count of active vehicles — used for pagination metadata.
    public int getActiveVehicleCount() throws SQLException {
        String sql = "SELECT COUNT(*) FROM active_parking";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            if (rs.next())
                return rs.getInt(1);
        }
        return 0;
    }

    // ── ACTIVE VEHICLES — FILTERED + PAGED ───────────────

    // Returns filtered active vehicles for a given page.
    // floor and type are optional — pass null to skip either filter.
    public List<Map<String, Object>> getActiveVehiclesFiltered(
            Integer floor, String type,
            int page, int pageSize) throws SQLException {

        int offset = (page - 1) * pageSize;
        StringBuilder sql = new StringBuilder(
                "SELECT ap.number_plate, ap.slot_id, ap.floor_num, "
                        + "ap.vehicle_type, ap.entry_time, ps.slot_num "
                        + "FROM active_parking ap "
                        + "JOIN parking_slots ps ON ap.slot_id = ps.id "
                        + "WHERE 1=1");

        if (floor != null)
            sql.append(" AND ap.floor_num = ?");
        if (type != null)
            sql.append(" AND ap.vehicle_type = ?");
        sql.append(" ORDER BY ap.entry_time DESC LIMIT ? OFFSET ?");

        List<Map<String, Object>> list = new ArrayList<>();
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            int idx = 1;
            if (floor != null)
                ps.setInt(idx++, floor);
            if (type != null)
                ps.setString(idx++, type.toLowerCase());
            ps.setInt(idx++, pageSize);
            ps.setInt(idx, offset);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("number_plate", rs.getString("number_plate"));
                    row.put("slot_id", rs.getInt("slot_id"));
                    row.put("slot_num", rs.getInt("slot_num"));
                    row.put("floor_num", rs.getInt("floor_num"));
                    row.put("vehicle_type", rs.getString("vehicle_type"));
                    row.put("entry_time", rs.getTimestamp("entry_time")
                            .toLocalDateTime().toString().replace("T", " "));
                    list.add(row);
                }
            }
        }
        return list;
    }

    // Returns total count of filtered active vehicles — used for pagination
    // metadata.
    public int getActiveVehiclesFilteredCount(
            Integer floor, String type) throws SQLException {

        StringBuilder sql = new StringBuilder(
                "SELECT COUNT(*) FROM active_parking WHERE 1=1");
        if (floor != null)
            sql.append(" AND floor_num = ?");
        if (type != null)
            sql.append(" AND vehicle_type = ?");

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {
            int idx = 1;
            if (floor != null)
                ps.setInt(idx++, floor);
            if (type != null)
                ps.setString(idx, type.toLowerCase());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getInt(1);
            }
        }
        return 0;
    }

    // ── SLOT LOCKING (FOR UPDATE SKIP LOCKED) ────────────
    //
    // All slot selection methods use SELECT ... FOR UPDATE SKIP LOCKED
    // inside a transaction that ends with INSERT INTO active_parking.
    //
    // This prevents race conditions where two threads pick the same slot:
    // Thread 1 locks slot 5 → Thread 2 skips it → picks slot 6.
    // The connection must stay open from SELECT through INSERT.
    // The Connection is passed in so the caller controls the transaction.

    // Locks the next available slot on a specific floor for the given vehicle type.
    public Slot lockNextFreeSlot(Connection con,
            int floor, String type) throws SQLException {

        String sql = "SELECT ps.id, ps.floor_num, ps.slot_num, ps.vehicle_type, "
                + "ps.allowed_mins, ps.rate_per_hr, ps.penalty_per_hr "
                + "FROM parking_slots ps "
                + "LEFT JOIN active_parking ap ON ps.id = ap.slot_id "
                + "WHERE ps.floor_num = ? AND ps.vehicle_type = ? "
                + "AND ap.slot_id IS NULL "
                + "ORDER BY ps.slot_num "
                + "LIMIT 1 "
                + "FOR UPDATE OF ps SKIP LOCKED";

        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ps.setString(2, type);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // Locks any free slot across all non-blocked floors for the given vehicle type.
    public Slot lockAnyFreeSlot(Connection con,
            String type,
            Set<Integer> blockedFloors) throws SQLException {

        StringBuilder sql = new StringBuilder(
                "SELECT ps.id, ps.floor_num, ps.slot_num, ps.vehicle_type, "
                        + "ps.allowed_mins, ps.rate_per_hr, ps.penalty_per_hr "
                        + "FROM parking_slots ps "
                        + "LEFT JOIN active_parking ap ON ps.id = ap.slot_id "
                        + "WHERE ps.vehicle_type = ? "
                        + "AND ap.slot_id IS NULL ");

        if (!blockedFloors.isEmpty()) {
            sql.append("AND ps.floor_num NOT IN (");
            for (int i = 0; i < blockedFloors.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(") ");
        }

        sql.append("ORDER BY ps.floor_num, ps.slot_num "
                + "LIMIT 1 FOR UPDATE OF ps SKIP LOCKED");

        try (PreparedStatement ps = con.prepareStatement(sql.toString())) {
            int idx = 1;
            ps.setString(idx++, type);
            for (int f : blockedFloors)
                ps.setInt(idx++, f);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // Locks a specific slot by floor and slot number for the given vehicle type.
    public Slot lockSpecificSlot(Connection con,
            int floor, int slotNum, String type) throws SQLException {

        String sql = "SELECT ps.id, ps.floor_num, ps.slot_num, ps.vehicle_type, "
                + "ps.allowed_mins, ps.rate_per_hr, ps.penalty_per_hr "
                + "FROM parking_slots ps "
                + "LEFT JOIN active_parking ap ON ps.id = ap.slot_id "
                + "WHERE ps.floor_num = ? AND ps.slot_num = ? "
                + "AND ps.vehicle_type = ? "
                + "AND ap.slot_id IS NULL "
                + "FOR UPDATE OF ps SKIP LOCKED";

        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ps.setInt(2, slotNum);
            ps.setString(3, type);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // Locks a slot by slot number across non-blocked floors for the given vehicle
    // type.
    public Slot lockSlotByNum(Connection con,
            int slotNum, String type,
            Set<Integer> blockedFloors) throws SQLException {

        StringBuilder sql = new StringBuilder(
                "SELECT ps.id, ps.floor_num, ps.slot_num, ps.vehicle_type, "
                        + "ps.allowed_mins, ps.rate_per_hr, ps.penalty_per_hr "
                        + "FROM parking_slots ps "
                        + "LEFT JOIN active_parking ap ON ps.id = ap.slot_id "
                        + "WHERE ps.slot_num = ? AND ps.vehicle_type = ? "
                        + "AND ap.slot_id IS NULL ");

        if (!blockedFloors.isEmpty()) {
            sql.append("AND ps.floor_num NOT IN (");
            for (int i = 0; i < blockedFloors.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(") ");
        }

        sql.append("LIMIT 1 FOR UPDATE OF ps SKIP LOCKED");

        try (PreparedStatement ps = con.prepareStatement(sql.toString())) {
            int idx = 1;
            ps.setInt(idx++, slotNum);
            ps.setString(idx++, type);
            for (int f : blockedFloors)
                ps.setInt(idx++, f);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // ── ATOMIC VEHICLE ENTRY ──────────────────────────────

    // Locks a slot and inserts into active_parking in a single transaction.
    // This is the only safe way to park a vehicle under concurrent access.
    // Returns the assigned Slot, or null if no suitable slot is available.
    public Slot atomicVehicleEntry(String plate,
            Integer floor, Integer slotNum,
            String type,
            Set<Integer> blockedFloors) throws SQLException {

        try (Connection con = pool.getConnection()) {
            con.setAutoCommit(false);
            try {
                Slot slot = null;

                boolean hasFloor = floor != null;
                boolean hasSlot = slotNum != null;

                if (hasFloor && hasSlot) {
                    slot = lockSpecificSlot(con, floor, slotNum, type);
                } else if (hasFloor) {
                    slot = lockNextFreeSlot(con, floor, type);
                } else if (hasSlot) {
                    slot = lockSlotByNum(con, slotNum, type, blockedFloors);
                } else {
                    slot = lockAnyFreeSlot(con, type, blockedFloors);
                }

                if (slot == null) {
                    con.rollback();
                    return null;
                }

                // Insert into active_parking within the same transaction.
                // The slot row lock is held until this commit.
                String insertActive = "INSERT INTO active_parking "
                        + "(number_plate, slot_id, floor_num, vehicle_type, entry_time) "
                        + "VALUES (?, ?, ?, ?, ?)";

                try (PreparedStatement ps = con.prepareStatement(insertActive)) {
                    ps.setString(1, plate);
                    ps.setInt(2, slot.id);
                    ps.setInt(3, slot.floor);
                    ps.setString(4, type);
                    ps.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
                    ps.executeUpdate();
                }

                con.commit();
                return slot;

            } catch (SQLException e) {
                con.rollback();
                throw e;
            } finally {
                con.setAutoCommit(true);
            }
        }
    }

    // ── FREE SLOT QUERIES (non-locking) ──────────────────

    // Returns the next free slot on a floor, excluding already occupied slot IDs.
    public Slot getNextFreeSlot(int floor,
            String type,
            Set<Integer> occupiedIds)
            throws SQLException {

        StringBuilder sql = new StringBuilder(
                "SELECT id, floor_num, slot_num, vehicle_type, "
                        + "allowed_mins, rate_per_hr, penalty_per_hr "
                        + "FROM parking_slots "
                        + "WHERE floor_num = ? AND vehicle_type = ?");

        if (!occupiedIds.isEmpty()) {
            sql.append(" AND id NOT IN (");
            for (int i = 0; i < occupiedIds.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(")");
        }

        sql.append(" LIMIT 1");

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            ps.setInt(1, floor);
            ps.setString(2, type);

            int idx = 3;
            for (int id : occupiedIds) {
                ps.setInt(idx++, id);
            }

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // Returns a slot by floor, slot number and vehicle type.
    public Slot getSlotByFloorAndNum(int floor,
            int slotNum,
            String type)
            throws SQLException {

        String sql = "SELECT id, floor_num, slot_num, vehicle_type, "
                + "allowed_mins, rate_per_hr, penalty_per_hr "
                + "FROM parking_slots "
                + "WHERE floor_num = ? "
                + "AND slot_num = ? "
                + "AND vehicle_type = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);
            ps.setInt(2, slotNum);
            ps.setString(3, type);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // Returns slot details by slot ID.
    public Slot getSlotDetails(int slotId) throws SQLException {
        String sql = "SELECT id, floor_num, slot_num, vehicle_type, "
                + "allowed_mins, rate_per_hr, penalty_per_hr "
                + "FROM parking_slots WHERE id = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, slotId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // ── ENTRY / EXIT ─────────────────────────────────────

    // Inserts a vehicle into active_parking when it enters.
    public void insertActiveVehicle(ActiveVehicle vehicle)
            throws SQLException {

        String sql = "INSERT INTO active_parking "
                + "(number_plate, slot_id, floor_num, "
                + "vehicle_type, entry_time) "
                + "VALUES (?, ?, ?, ?, ?)";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setString(1, vehicle.numberPlate);
            ps.setInt(2, vehicle.slotId);
            ps.setInt(3, vehicle.floor);
            ps.setString(4, vehicle.vehicleType);
            ps.setTimestamp(5, Timestamp.valueOf(vehicle.entryTime));
            ps.executeUpdate();
        }
    }

    // Removes a vehicle from active_parking and writes the billing record
    // to parking_history in a single atomic transaction.
    // Also clears the stats cache so reports stay accurate after an exit.
    public void processExit(BillResult bill) throws SQLException {

        String deleteActive = "DELETE FROM active_parking WHERE number_plate = ?";

        String insertHistory = "INSERT INTO parking_history "
                + "(number_plate, slot_id, slot_num, floor_num, vehicle_type, "
                + "entry_time, exit_time, exit_date, allowed_mins, total_mins, "
                + "extra_mins, total_amount, penalty_amount) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection con = pool.getConnection()) {
            con.setAutoCommit(false);
            try {
                try (PreparedStatement ps1 = con.prepareStatement(deleteActive)) {
                    ps1.setString(1, bill.numberPlate);
                    ps1.executeUpdate();
                }

                try (PreparedStatement ps2 = con.prepareStatement(insertHistory)) {
                    ps2.setString(1, bill.numberPlate);
                    ps2.setInt(2, bill.slotId);
                    ps2.setInt(3, bill.slotNum);
                    ps2.setInt(4, bill.floor);
                    ps2.setString(5, bill.vehicleType);
                    ps2.setTimestamp(6, Timestamp.valueOf(bill.entryTime));
                    ps2.setTimestamp(7, Timestamp.valueOf(bill.exitTime));
                    ps2.setDate(8, java.sql.Date.valueOf(bill.exitTime.toLocalDate()));
                    ps2.setInt(9, bill.allowedMins);
                    ps2.setLong(10, bill.totalMins);
                    ps2.setLong(11, bill.extraMins);
                    ps2.setDouble(12, bill.totalAmount);
                    ps2.setDouble(13, bill.penaltyAmount);
                    ps2.executeUpdate();
                }

                con.commit();
                statsCache.clear();

            } catch (SQLException e) {
                con.rollback();
                throw e;
            } finally {
                con.setAutoCommit(true);
            }
        }
    }

    // ── SLOT MANAGEMENT ──────────────────────────────────

    // Inserts a new parking slot and returns the generated slot ID.
    public int insertSlot(int floor,
            int slotNum,
            String type,
            int allowedMins,
            double rate,
            double penalty) throws SQLException {

        String sql = "INSERT INTO parking_slots "
                + "(floor_num, slot_num, vehicle_type, "
                + "allowed_mins, rate_per_hr, penalty_per_hr) "
                + "VALUES (?, ?, ?, ?, ?, ?) "
                + "RETURNING id";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);
            ps.setInt(2, slotNum);
            ps.setString(3, type);
            ps.setInt(4, allowedMins);
            ps.setDouble(5, rate);
            ps.setDouble(6, penalty);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getInt(1);
            }
        }
        return -1;
    }

    // Deletes a slot by ID. Throws an exception if a vehicle is currently parked
    // there.
    public void deleteSlot(int slotId) throws SQLException {
        String checkSql = "SELECT COUNT(*) FROM active_parking WHERE slot_id = ?";
        try (Connection con = pool.getConnection();
                PreparedStatement check = con.prepareStatement(checkSql)) {
            check.setInt(1, slotId);
            try (ResultSet rs = check.executeQuery()) {
                if (rs.next() && rs.getInt(1) > 0) {
                    throw new SQLException("SLOT_OCCUPIED: Cannot delete slot — a vehicle is currently parked here");
                }
            }
        }

        String sql = "DELETE FROM parking_slots WHERE id = ?";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, slotId);
            ps.executeUpdate();
        }
    }

    // Updates billing config (allowed time, rate, penalty) for an existing slot.
    public void updateSlot(int slotId,
            int allowedMins,
            double rate,
            double penalty) throws SQLException {

        String sql = "UPDATE parking_slots "
                + "SET allowed_mins = ?, "
                + "rate_per_hr = ?, "
                + "penalty_per_hr = ? "
                + "WHERE id = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, allowedMins);
            ps.setDouble(2, rate);
            ps.setDouble(3, penalty);
            ps.setInt(4, slotId);
            ps.executeUpdate();
        }
    }

    // Returns true if a slot with the given floor, slot number and vehicle type
    // exists.
    public boolean slotExists(int floor,
            int slotNum,
            String type) throws SQLException {

        String sql = "SELECT 1 FROM parking_slots "
                + "WHERE floor_num = ? "
                + "AND slot_num = ? "
                + "AND vehicle_type = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);
            ps.setInt(2, slotNum);
            ps.setString(3, type);

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    // Returns true if a vehicle is currently parked in the given slot.
    public boolean isSlotOccupied(int slotId) throws SQLException {
        String sql = "SELECT 1 FROM active_parking WHERE slot_id = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, slotId);

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    // Returns all slots on a floor with their occupancy status and vehicle plate if
    // occupied.
    

    // Returns full slot data for a floor.
    public List<Map<String, Object>> getSlotsByFloor(int floor) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        String sql = """
            SELECT 
                ps.id,
                ps.floor_num,
                ps.slot_num,
                ps.vehicle_type,
                ps.allowed_mins,
                ps.rate_per_hr,
                ps.penalty_per_hr,
                ps.slot_row,
                ps.col_start,
                ps.col_end,
                ps.size,
                ap.number_plate,
                CASE WHEN ap.slot_id IS NOT NULL THEN 'OCCUPIED' ELSE 'FREE' END as status
            FROM parking_slots ps
            LEFT JOIN active_parking ap ON ps.id = ap.slot_id
            WHERE ps.floor_num = ? AND ps.is_active = true
            ORDER BY ps.slot_row, ps.col_start
        """;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("slot_id", rs.getInt("id"));
                row.put("floor_num", rs.getInt("floor_num"));
                row.put("slot_num", rs.getInt("slot_num"));
                row.put("vehicle_type", rs.getString("vehicle_type"));
                row.put("allowed_mins", rs.getInt("allowed_mins"));
                row.put("rate_per_hr", rs.getDouble("rate_per_hr"));
                row.put("penalty_per_hr", rs.getDouble("penalty_per_hr"));
                row.put("slot_row", rs.getInt("slot_row"));
                row.put("col_start", rs.getInt("col_start"));
                row.put("col_end", rs.getInt("col_end"));
                row.put("size", rs.getInt("size"));
                row.put("status", rs.getString("status"));
                row.put("number_plate", rs.getString("number_plate"));
                list.add(row);
            }
        }
        return list;
    }

    // Returns full slot data for a floor.
    public List<Slot> getSlotsByFloorObjects(int floor) throws Exception {
        List<Slot> list = new ArrayList<>();
        String sql = "SELECT id, floor_num, slot_num, vehicle_type, allowed_mins, rate_per_hr, penalty_per_hr, slot_row, col_start, col_end, size FROM parking_slots WHERE floor_num = ? AND is_active = true ORDER BY slot_row, col_start";
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int row = rs.getInt("slot_row");
            System.out.println("ROW DEBUG: " + row);
                list.add(new Slot(
                    rs.getInt("id"),
                    rs.getInt("floor_num"),
                    rs.getInt("slot_num"),
                    rs.getString("vehicle_type"),
                    rs.getInt("allowed_mins"),
                    rs.getDouble("rate_per_hr"),
                    rs.getDouble("penalty_per_hr"),
                    rs.getInt("slot_row"),
                    rs.getInt("col_start"),
                    rs.getInt("col_end"),
                    rs.getInt("size")
                ));
            }
        }
        return list;
    }

    // ── FLOOR MANAGEMENT ─────────────────────────────────

    // Blocks a floor with a given reason. Updates reason if the floor is already
    // blocked.
    public void blockFloor(int floor,
            String reason) throws SQLException {

        String sql = "INSERT INTO blocked_floor (floor_num, reason) "
                + "VALUES (?, ?) "
                + "ON CONFLICT (floor_num) DO UPDATE "
                + "SET reason = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);
            ps.setString(2, reason);
            ps.setString(3, reason);
            ps.executeUpdate();
        }
    }

    // Unblocks a floor by removing it from the blocked_floor table.
    public void unblockFloor(int floor) throws SQLException {
        String sql = "DELETE FROM blocked_floor WHERE floor_num = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);
            ps.executeUpdate();
        }
    }

    // Returns true if the given floor has at least one slot defined.
    public boolean floorHasSlots(int floor) throws SQLException {
        String sql = "SELECT 1 FROM parking_slots "
                + "WHERE floor_num = ? LIMIT 1";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    // Returns a sorted list of all floor numbers that have at least one slot.
    public List<Integer> getAllFloorNums() throws SQLException {
        String sql = "SELECT DISTINCT floor_num "
                + "FROM parking_slots ORDER BY floor_num";

        List<Integer> floors = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                floors.add(rs.getInt("floor_num"));
            }
        }
        return floors;
    }

    // Returns total slot count on a floor for a specific vehicle type.
    public int getTotalSlotCount(int floor,
            String type) throws SQLException {

        String sql = "SELECT COUNT(*) FROM parking_slots "
                + "WHERE floor_num = ? AND vehicle_type = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);
            ps.setString(2, type);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getInt(1);
            }
        }
        return 0;
    }

    // Returns the total number of slots on a given floor across all vehicle types.
    public int getTotalSlotCountByFloor(int floor) throws SQLException {
        String sql = "SELECT COUNT(*) FROM parking_slots "
                + "WHERE floor_num = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getInt(1);
            }
        }
        return 0;
    }

    // ── DISTINCT VEHICLE TYPES ────────────────────────────

    // Returns all vehicle types that have at least one slot defined.
    // Fully dynamic — new types appear automatically once a slot is created for
    // them.
    public List<String> getDistinctVehicleTypes() throws SQLException {
        String sql = "SELECT DISTINCT vehicle_type "
                + "FROM parking_slots ORDER BY vehicle_type";
        List<String> types = new ArrayList<>();
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                types.add(rs.getString("vehicle_type"));
            }
        }
        return types;
    }

    // ── DISTINCT ALLOWED MINS ─────────────────────────────

    // Returns distinct allowed_mins values from the last 30 days of
    // parking_history.
    // Queried from history so deleted slots still appear in historical data.
    // Limited to 20 values for the dropdown filter.
    public List<Integer> getDistinctAllowedMins() throws SQLException {
        String sql = "SELECT DISTINCT allowed_mins "
                + "FROM parking_history "
                + "WHERE exit_date >= CURRENT_DATE - INTERVAL '30 days' "
                + "ORDER BY allowed_mins "
                + "LIMIT 20";
        List<Integer> mins = new ArrayList<>();
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                mins.add(rs.getInt("allowed_mins"));
            }
        }
        return mins;
    }

    // ── HISTORY ──────────────────────────────────────────

    // Returns full parking history for a specific vehicle plate, newest first.
    public List<BillResult> getParkingHistory(String plate)
            throws SQLException {

        String sql = "SELECT ph.slot_id, ph.slot_num, ph.floor_num, ph.vehicle_type, "
                + "ph.entry_time, ph.exit_time, ph.allowed_mins, "
                + "ph.total_mins, ph.extra_mins, "
                + "ph.total_amount, ph.penalty_amount "
                + "FROM parking_history ph "
                + "WHERE ph.number_plate = ? "
                + "ORDER BY ph.exit_time DESC";

        List<BillResult> history = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setString(1, plate);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    history.add(new BillResult(
                            plate,
                            rs.getInt("floor_num"),
                            rs.getInt("slot_id"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getTimestamp("entry_time").toLocalDateTime(),
                            rs.getTimestamp("exit_time").toLocalDateTime(),
                            rs.getInt("allowed_mins"),
                            rs.getLong("total_mins"),
                            rs.getLong("extra_mins"),
                            rs.getDouble("total_amount"),
                            rs.getDouble("penalty_amount")));
                }
            }
        }
        return history;
    }

    // Returns one page of all parking history records, newest first.
    // page is 1-based. pageSize = 50. Never loads all rows — safe on large tables.
    public List<BillResult> getAllHistory(int page, int pageSize) throws SQLException {
        int offset = (page - 1) * pageSize;
        String sql = "SELECT ph.slot_id, ph.slot_num, ph.number_plate, ph.floor_num, "
                + "ph.vehicle_type, ph.entry_time, ph.exit_time, "
                + "ph.allowed_mins, ph.total_mins, ph.extra_mins, "
                + "ph.total_amount, ph.penalty_amount "
                + "FROM parking_history ph "
                + "ORDER BY ph.exit_time DESC "
                + "LIMIT ? OFFSET ?";

        List<BillResult> history = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, pageSize);
            ps.setInt(2, offset);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    history.add(new BillResult(
                            rs.getString("number_plate"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_id"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getTimestamp("entry_time").toLocalDateTime(),
                            rs.getTimestamp("exit_time").toLocalDateTime(),
                            rs.getInt("allowed_mins"),
                            rs.getLong("total_mins"),
                            rs.getLong("extra_mins"),
                            rs.getDouble("total_amount"),
                            rs.getDouble("penalty_amount")));
                }
            }
        }
        return history;
    }

    // ── AUTH ─────────────────────────────────────────────

    // Returns user credentials and status by username, or null if not found.
    public String[] getUserByUsername(String username)
            throws SQLException {

        String sql = "SELECT username, password, role, status "
                + "FROM users WHERE username = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setString(1, username);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new String[] {
                            rs.getString("username"),
                            rs.getString("password"),
                            rs.getString("role"),
                            rs.getString("status")
                    };
                }
            }
        }
        return null;
    }

    // ── FREE SLOT QUERIES (non-locking, availability checks) ─────────────

    // Returns a free slot by slot number, excluding occupied IDs and blocked
    // floors.
    public Slot getSlotBySlotNum(int slotNum,
            String type,
            Set<Integer> occupiedIds,
            Set<Integer> blockedFloors)
            throws SQLException {

        StringBuilder sql = new StringBuilder(
                "SELECT id, floor_num, slot_num, vehicle_type, "
                        + "allowed_mins, rate_per_hr, penalty_per_hr "
                        + "FROM parking_slots "
                        + "WHERE slot_num = ? AND vehicle_type = ?");

        if (!occupiedIds.isEmpty()) {
            sql.append(" AND id NOT IN (");
            for (int i = 0; i < occupiedIds.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(")");
        }

        if (!blockedFloors.isEmpty()) {
            sql.append(" AND floor_num NOT IN (");
            for (int i = 0; i < blockedFloors.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(")");
        }

        sql.append(" LIMIT 1");

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            int idx = 1;
            ps.setInt(idx++, slotNum);
            ps.setString(idx++, type);

            for (int id : occupiedIds) {
                ps.setInt(idx++, id);
            }

            for (int floor : blockedFloors) {
                ps.setInt(idx++, floor);
            }

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // Returns all free slot numbers on a floor for a vehicle type,
    // excluding already occupied slot IDs.
    public List<Integer> getFreeSlotNums(int floor,
            String type,
            Set<Integer> occupiedIds)
            throws SQLException {

        StringBuilder sql = new StringBuilder(
                "SELECT slot_num FROM parking_slots "
                        + "WHERE floor_num = ? AND vehicle_type = ?");

        if (!occupiedIds.isEmpty()) {
            sql.append(" AND id NOT IN (");
            for (int i = 0; i < occupiedIds.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(")");
        }

        sql.append(" ORDER BY slot_num");

        List<Integer> freeSlots = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            int idx = 1;
            ps.setInt(idx++, floor);
            ps.setString(idx++, type);

            for (int id : occupiedIds) {
                ps.setInt(idx++, id);
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    freeSlots.add(rs.getInt("slot_num"));
                }
            }
        }
        return freeSlots;
    }

    // Returns any available slot for the given vehicle type,
    // excluding occupied IDs and blocked floors.
    public Slot getAnyFreeSlot(String type,
            Set<Integer> occupiedIds,
            Set<Integer> blockedFloors)
            throws SQLException {

        StringBuilder sql = new StringBuilder(
                "SELECT id, floor_num, slot_num, vehicle_type, "
                        + "allowed_mins, rate_per_hr, penalty_per_hr "
                        + "FROM parking_slots "
                        + "WHERE vehicle_type = ?");

        if (!occupiedIds.isEmpty()) {
            sql.append(" AND id NOT IN (");
            for (int i = 0; i < occupiedIds.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(")");
        }

        if (!blockedFloors.isEmpty()) {
            sql.append(" AND floor_num NOT IN (");
            for (int i = 0; i < blockedFloors.size(); i++) {
                sql.append(i == 0 ? "?" : ",?");
            }
            sql.append(")");
        }

        sql.append(" LIMIT 1");

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            int idx = 1;
            ps.setString(idx++, type);

            for (int id : occupiedIds) {
                ps.setInt(idx++, id);
            }

            for (int floor : blockedFloors) {
                ps.setInt(idx++, floor);
            }

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"));
                }
            }
        }
        return null;
    }

    // ── USER MANAGEMENT ──────────────────────────────────

    // Registers a new user with 'pending' status awaiting admin approval.
    public void registerUser(String username, String password, String role)
            throws SQLException {
        String sql = "INSERT INTO users (username, password, role, status, requested_at) "
                + "VALUES (?, ?, ?, 'pending', NOW())";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, username);
            ps.setString(2, password);
            ps.setString(3, role);
            ps.executeUpdate();
        }
    }

    // Returns a list of users with 'pending' status, ordered by registration date.
    public List<String[]> getPendingUsers() throws SQLException {
        String sql = "SELECT id, username, role, requested_at "
                + "FROM users WHERE status = 'pending' ORDER BY requested_at ASC";
        List<String[]> list = new ArrayList<>();
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new String[] {
                        String.valueOf(rs.getInt("id")),
                        rs.getString("username"),
                        rs.getString("role"),
                        rs.getString("requested_at")
                });
            }
        }
        return list;
    }

    // Approves a pending user by setting their status to 'active'.
    public void approveUser(int userId) throws SQLException {
        String sql = "UPDATE users SET status = 'active' WHERE id = ?";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, userId);
            ps.executeUpdate();
        }
    }

    // Rejects and removes a pending user registration.
    public void rejectUser(int userId) throws SQLException {
        String sql = "DELETE FROM users WHERE id = ?";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, userId);
            ps.executeUpdate();
        }
    }

    // Returns true if the given username belongs to an active admin.
    // Used to verify that the approver has admin privileges.
    public boolean canApproveAdmin(String username) throws SQLException {
        String sql = "SELECT 1 FROM users "
                + "WHERE username = ? AND role = 'admin' AND status = 'active'";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    // Returns all users in the system ordered by ID.
    public List<String[]> getAllUsers() throws SQLException {
        String sql = "SELECT id, username, role, status, requested_at "
                + "FROM users ORDER BY id ASC";
        List<String[]> list = new ArrayList<>();
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                list.add(new String[] {
                        String.valueOf(rs.getInt("id")),
                        rs.getString("username"),
                        rs.getString("role"),
                        rs.getString("status"),
                        rs.getString("requested_at") == null
                                ? "—"
                                : rs.getString("requested_at")
                });
            }
        }
        return list;
    }

    // ── FILTERED HISTORY ─────────────────────────────────

    // Backward-compatible wrapper — delegates to getFilteredHistoryPage with
    // default pagination.
    public List<Map<String, Object>> getFilteredHistory(
            Integer floor, Integer slot, String vehicleType, String plate,
            LocalDateTime from, LocalDateTime to, Boolean penaltyOnly,
            Double minPenalty, Double maxPenalty,
            Double minAmount, Double maxAmount) throws SQLException {
        return getFilteredHistoryPage(
                floor, slot, vehicleType, plate, from, to, penaltyOnly,
                minPenalty, maxPenalty, minAmount, maxAmount,
                null, null, null, null, 1, 50);
    }

    // Returns one page of filtered history records.
    // Uses a BRIN index on exit_date to prune irrelevant rows before applying
    // LIMIT/OFFSET — making pagination safe even on very large tables.
    // Defaults to the last 7 days if no date range is provided.
    public List<Map<String, Object>> getFilteredHistoryPage(
            Integer floor,
            Integer slot,
            String vehicleType,
            String plate,
            LocalDateTime from,
            LocalDateTime to,
            Boolean penaltyOnly,
            Double minPenalty,
            Double maxPenalty,
            Double minAmount,
            Double maxAmount,
            Integer allowedMins,
            Integer minTotalMins,
            Integer maxTotalMins,
            Integer minExtraMins,
            int page,
            int pageSize) throws SQLException {

        if (from == null && to == null) {
            from = LocalDateTime.now().minusDays(7);
            to = LocalDateTime.now();
        }

        int offset = (page - 1) * pageSize;

        StringBuilder sql = new StringBuilder(
                "SELECT id, number_plate, slot_id, slot_num, floor_num, "
                        + "vehicle_type, entry_time, exit_time, "
                        + "allowed_mins, total_mins, extra_mins, "
                        + "total_amount, penalty_amount "
                        + "FROM parking_history WHERE 1=1");

        List<Object> params = new ArrayList<>();

        // Date range filter first — BRIN index on exit_date prunes irrelevant disk
        // blocks.
        if (from != null) {
            sql.append(" AND exit_date >= ?");
            params.add(java.sql.Date.valueOf(from.toLocalDate()));
        }
        if (to != null) {
            sql.append(" AND exit_date <= ?");
            params.add(java.sql.Date.valueOf(to.toLocalDate()));
        }
        if (floor != null) {
            sql.append(" AND floor_num = ?");
            params.add(floor);
        }
        if (slot != null) {
            sql.append(" AND slot_num = ?");
            params.add(slot);
        }
        if (vehicleType != null && !vehicleType.isEmpty()) {
            sql.append(" AND vehicle_type = ?");
            params.add(vehicleType.toLowerCase());
        }
        if (plate != null && !plate.isEmpty()) {
            sql.append(" AND number_plate ILIKE ?");
            params.add("%" + plate.toUpperCase() + "%");
        }
        if (penaltyOnly != null && penaltyOnly) {
            sql.append(" AND extra_mins > 0");
        }
        if (minPenalty != null) {
            sql.append(" AND penalty_amount >= ?");
            params.add(minPenalty);
        }
        if (maxPenalty != null) {
            sql.append(" AND penalty_amount <= ?");
            params.add(maxPenalty);
        }
        if (minAmount != null) {
            sql.append(" AND total_amount >= ?");
            params.add(minAmount);
        }
        if (maxAmount != null) {
            sql.append(" AND total_amount <= ?");
            params.add(maxAmount);
        }
        if (allowedMins != null) {
            sql.append(" AND allowed_mins = ?");
            params.add(allowedMins);
        }
        if (minTotalMins != null) {
            sql.append(" AND total_mins >= ?");
            params.add(minTotalMins);
        }
        if (maxTotalMins != null) {
            sql.append(" AND total_mins <= ?");
            params.add(maxTotalMins);
        }
        if (minExtraMins != null) {
            sql.append(" AND extra_mins >= ?");
            params.add(minExtraMins);
        }

        sql.append(" ORDER BY exit_date DESC, id DESC LIMIT ? OFFSET ?");
        params.add(pageSize);
        params.add(offset);

        List<Map<String, Object>> results = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                Object p = params.get(i);
                if (p instanceof Integer)
                    ps.setInt(i + 1, (Integer) p);
                else if (p instanceof Long)
                    ps.setLong(i + 1, (Long) p);
                else if (p instanceof Double)
                    ps.setDouble(i + 1, (Double) p);
                else if (p instanceof java.sql.Date)
                    ps.setDate(i + 1, (java.sql.Date) p);
                else if (p instanceof Timestamp)
                    ps.setTimestamp(i + 1, (Timestamp) p);
                else
                    ps.setString(i + 1, p.toString());
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    row.put("id", rs.getLong("id"));
                    row.put("number_plate", rs.getString("number_plate"));
                    row.put("floor", rs.getInt("floor_num"));
                    row.put("slot_num", rs.getObject("slot_num"));
                    row.put("vehicle_type", rs.getString("vehicle_type"));
                    row.put("entry_time", rs.getTimestamp("entry_time")
                            .toLocalDateTime().toString().replace("T", " "));
                    row.put("exit_time", rs.getTimestamp("exit_time")
                            .toLocalDateTime().toString().replace("T", " "));
                    row.put("allowed_mins", rs.getInt("allowed_mins"));
                    row.put("total_mins", rs.getInt("total_mins"));
                    row.put("extra_mins", rs.getInt("extra_mins"));
                    row.put("total_amount", rs.getDouble("total_amount"));
                    row.put("penalty_amount", rs.getDouble("penalty_amount"));
                    results.add(row);
                }
            }
        }
        return results;
    }

    // ── FILTERED HISTORY STATS ────────────────────────────

    // Returns aggregated stats (count, total revenue, total penalty) for a filtered
    // history query. Results are cached for 60 seconds per unique filter
    // combination.
    @SuppressWarnings("unchecked")
    public Map<String, Object> getFilteredHistoryStats(
            Integer floor,
            Integer slot,
            String vehicleType,
            String plate,
            LocalDateTime from,
            LocalDateTime to,
            Boolean penaltyOnly,
            Double minPenalty,
            Double maxPenalty,
            Double minAmount,
            Double maxAmount,
            Integer allowedMins,
            Integer minTotalMins,
            Integer maxTotalMins,
            Integer minExtraMins) throws SQLException {

        if (from == null && to == null) {
            from = LocalDateTime.now().minusDays(7);
            to = LocalDateTime.now();
        }

        // Build a unique cache key from all active filter values.
        String cacheKey = floor + "|" + slot + "|" + vehicleType + "|"
                + plate + "|" + from + "|" + to + "|" + penaltyOnly + "|"
                + minPenalty + "|" + maxPenalty + "|" + minAmount + "|" + maxAmount
                + "|" + allowedMins + "|" + minTotalMins + "|" + maxTotalMins
                + "|" + minExtraMins;

        // Return cached result if still within TTL.
        Object[] cached = statsCache.get(cacheKey);
        if (cached != null) {
            long expiry = (long) cached[1];
            if (System.currentTimeMillis() < expiry) {
                return (Map<String, Object>) cached[0];
            }
            statsCache.remove(cacheKey);
        }

        // Cache miss — run the aggregation query.
        StringBuilder sql = new StringBuilder(
                "SELECT COUNT(*) AS total_count, "
                        + "COALESCE(SUM(total_amount), 0) AS total_revenue, "
                        + "COALESCE(SUM(penalty_amount), 0) AS total_penalty "
                        + "FROM parking_history WHERE 1=1");

        List<Object> params = new ArrayList<>();

        if (from != null) {
            sql.append(" AND exit_date >= ?");
            params.add(java.sql.Date.valueOf(from.toLocalDate()));
        }
        if (to != null) {
            sql.append(" AND exit_date <= ?");
            params.add(java.sql.Date.valueOf(to.toLocalDate()));
        }
        if (floor != null) {
            sql.append(" AND floor_num = ?");
            params.add(floor);
        }
        if (slot != null) {
            sql.append(" AND slot_num = ?");
            params.add(slot);
        }
        if (vehicleType != null && !vehicleType.isEmpty()) {
            sql.append(" AND vehicle_type = ?");
            params.add(vehicleType.toLowerCase());
        }
        if (plate != null && !plate.isEmpty()) {
            sql.append(" AND number_plate ILIKE ?");
            params.add("%" + plate.toUpperCase() + "%");
        }
        if (penaltyOnly != null && penaltyOnly) {
            sql.append(" AND extra_mins > 0");
        }
        if (minPenalty != null) {
            sql.append(" AND penalty_amount >= ?");
            params.add(minPenalty);
        }
        if (maxPenalty != null) {
            sql.append(" AND penalty_amount <= ?");
            params.add(maxPenalty);
        }
        if (minAmount != null) {
            sql.append(" AND total_amount >= ?");
            params.add(minAmount);
        }
        if (maxAmount != null) {
            sql.append(" AND total_amount <= ?");
            params.add(maxAmount);
        }
        if (allowedMins != null) {
            sql.append(" AND allowed_mins = ?");
            params.add(allowedMins);
        }
        if (minTotalMins != null) {
            sql.append(" AND total_mins >= ?");
            params.add(minTotalMins);
        }
        if (maxTotalMins != null) {
            sql.append(" AND total_mins <= ?");
            params.add(maxTotalMins);
        }
        if (minExtraMins != null) {
            sql.append(" AND extra_mins >= ?");
            params.add(minExtraMins);
        }

        Map<String, Object> stats = new HashMap<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                Object p = params.get(i);
                if (p instanceof Integer)
                    ps.setInt(i + 1, (Integer) p);
                else if (p instanceof Double)
                    ps.setDouble(i + 1, (Double) p);
                else if (p instanceof java.sql.Date)
                    ps.setDate(i + 1, (java.sql.Date) p);
                else if (p instanceof Timestamp)
                    ps.setTimestamp(i + 1, (Timestamp) p);
                else
                    ps.setString(i + 1, p.toString());
            }

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    stats.put("total_count", rs.getLong("total_count"));
                    stats.put("total_revenue", rs.getDouble("total_revenue"));
                    stats.put("total_penalty", rs.getDouble("total_penalty"));
                }
            }
        }

        // Store result in cache with expiry timestamp.
        statsCache.put(cacheKey, new Object[] { stats,
                System.currentTimeMillis() + CACHE_TTL_MS });

        return stats;
    }

    // ── FLOOR REPORT ─────────────────────────────────────

    // Returns per-vehicle-type stats for a floor:
    // total vehicles, revenue, penalty, avg and max duration.
    // Appends a grand total "ALL" row at the end.
    // Results are cached for 60 seconds.
    public List<Map<String, Object>> getFloorReport(
            int floor,
            LocalDateTime from,
            LocalDateTime to) throws SQLException {

        String key = "floor:" + floor + ":" + from + ":" + to;

        Object[] cached = statsCache.get(key);
        if (cached != null && (long) cached[1] > System.currentTimeMillis()) {
            return (List<Map<String, Object>>) cached[0];
        }

        StringBuilder sql = new StringBuilder(
                "SELECT vehicle_type, "
                        + "COUNT(*)                          AS total_vehicles, "
                        + "COALESCE(SUM(total_amount),   0)  AS total_revenue, "
                        + "COALESCE(SUM(penalty_amount), 0)  AS total_penalty, "
                        + "COALESCE(AVG(total_mins),     0)  AS avg_duration_mins, "
                        + "COALESCE(MAX(total_mins),     0)  AS max_duration_mins "
                        + "FROM parking_history "
                        + "WHERE floor_num = ?");

        List<Object> params = new ArrayList<>();
        params.add(floor);

        if (from != null) {
            sql.append(" AND exit_date >= ?");
            params.add(java.sql.Date.valueOf(from.toLocalDate()));
        }
        if (to != null) {
            sql.append(" AND exit_date <= ?");
            params.add(java.sql.Date.valueOf(to.toLocalDate()));
        }

        sql.append(" GROUP BY vehicle_type ORDER BY vehicle_type");

        List<Map<String, Object>> rows = new ArrayList<>();

        long grandVehicles = 0;
        double grandRevenue = 0;
        double grandPenalty = 0;
        double grandAvgSum = 0;
        long grandMax = 0;

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                Object p = params.get(i);
                if (p instanceof Integer)
                    ps.setInt(i + 1, (Integer) p);
                else if (p instanceof java.sql.Date)
                    ps.setDate(i + 1, (java.sql.Date) p);
                else
                    ps.setString(i + 1, p.toString());
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long tv = rs.getLong("total_vehicles");
                    double tr = rs.getDouble("total_revenue");
                    double tp = rs.getDouble("total_penalty");
                    double avg = rs.getDouble("avg_duration_mins");
                    long mx = rs.getLong("max_duration_mins");

                    Map<String, Object> row = new HashMap<>();
                    row.put("vehicle_type", rs.getString("vehicle_type"));
                    row.put("total_vehicles", tv);
                    row.put("total_revenue", Math.round(tr * 100.0) / 100.0);
                    row.put("total_penalty", Math.round(tp * 100.0) / 100.0);
                    row.put("avg_duration_mins", Math.round(avg * 10.0) / 10.0);
                    row.put("max_duration_mins", mx);
                    rows.add(row);

                    grandVehicles += tv;
                    grandRevenue += tr;
                    grandPenalty += tp;
                    grandAvgSum += avg * tv; // weighted average across types
                    if (mx > grandMax)
                        grandMax = mx;
                }
            }
        }

        // Grand total summary row across all vehicle types.
        Map<String, Object> total = new HashMap<>();
        total.put("vehicle_type", "ALL");
        total.put("total_vehicles", grandVehicles);
        total.put("total_revenue", Math.round(grandRevenue * 100.0) / 100.0);
        total.put("total_penalty", Math.round(grandPenalty * 100.0) / 100.0);
        total.put("avg_duration_mins", grandVehicles > 0
                ? Math.round((grandAvgSum / grandVehicles) * 10.0) / 10.0
                : 0.0);
        total.put("max_duration_mins", grandMax);
        rows.add(total);

        statsCache.put(key, new Object[] {
                rows,
                System.currentTimeMillis() + CACHE_TTL_MS
        });

        return rows;
    }

    // Returns floor reports for all floors grouped by floor number.
    // Defaults to the last 7 days if no date range is provided.
    // Floors with no history are included as empty lists.
    public Map<Integer, List<Map<String, Object>>> getAllFloorReports(
            LocalDateTime from,
            LocalDateTime to) throws SQLException {

        String key = "all_floors:" +
                (from != null ? from.toLocalDate() : "null") + ":" +
                (to != null ? to.toLocalDate() : "null");

        StringBuilder sql = new StringBuilder(
                "SELECT floor_num, vehicle_type, "
                        + "COUNT(*) AS total_vehicles, "
                        + "COALESCE(SUM(total_amount),0) AS total_revenue, "
                        + "COALESCE(SUM(penalty_amount),0) AS total_penalty, "
                        + "COALESCE(AVG(total_mins),0) AS avg_duration_mins, "
                        + "COALESCE(MAX(total_mins),0) AS max_duration_mins "
                        + "FROM parking_history WHERE 1=1");

        List<Object> params = new ArrayList<>();

        if (from != null) {
            sql.append(" AND exit_date >= ?");
            params.add(java.sql.Date.valueOf(from.toLocalDate()));
        }
        if (to != null) {
            sql.append(" AND exit_date <= ?");
            params.add(java.sql.Date.valueOf(to.toLocalDate()));
        }

        sql.append(" GROUP BY floor_num, vehicle_type ORDER BY floor_num");

        Map<Integer, List<Map<String, Object>>> result = new HashMap<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                Object p = params.get(i);
                if (p instanceof java.sql.Date)
                    ps.setDate(i + 1, (java.sql.Date) p);
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int floorNum = rs.getInt("floor_num");

                    Map<String, Object> row = new HashMap<>();
                    row.put("vehicle_type", rs.getString("vehicle_type"));
                    row.put("total_vehicles", rs.getLong("total_vehicles"));
                    row.put("total_revenue", rs.getDouble("total_revenue"));
                    row.put("total_penalty", rs.getDouble("total_penalty"));
                    row.put("avg_duration_mins", rs.getDouble("avg_duration_mins"));
                    row.put("max_duration_mins", rs.getLong("max_duration_mins"));

                    result.computeIfAbsent(floorNum, k -> new ArrayList<>()).add(row);
                }
            }
        }

        // Ensure all floors (1–20) are present in the result, even if they have no
        // history.
        int totalFloors = 20;
        for (int i = 1; i <= totalFloors; i++) {
            result.computeIfAbsent(i, k -> new ArrayList<>());
        }

        return result;
    }

    // ── STARTUP: VEHICLE SIZES ────────────────────────────

    // Loads the vehicle_size table into a map at startup.
    // Used to populate ParkingStore.vehicleSizes.
    public Map<String, Integer> loadVehicleSizes()
            throws SQLException {

        String sql = "SELECT vehicle_type, size FROM vehicle_size";
        Map<String, Integer> sizes = new HashMap<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                sizes.put(
                        rs.getString("vehicle_type"),
                        rs.getInt("size"));
            }
        }
        return sizes;
    }

    // ── STARTUP: PARKING FLOORS ───────────────────────────

    // Loads all floor grid configurations at startup.
    // Used to initialise gridMap dimensions in ParkingStore.
    public List<Map<String, Integer>> loadParkingFloors()
            throws SQLException {

        String sql = "SELECT floor_num, total_rows, total_cols, "
                + "path_width, slot_col_start, slot_col_end, "
                + "entry_row, entry_col, exit_row, exit_col "
                + "FROM parking_floors "
                + "ORDER BY floor_num";

        List<Map<String, Integer>> floors = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                Map<String, Integer> row = new HashMap<>();
                row.put("floor_num", rs.getInt("floor_num"));
                row.put("total_rows", rs.getInt("total_rows"));
                row.put("total_cols", rs.getInt("total_cols"));
                row.put("path_width", rs.getInt("path_width"));
                row.put("slot_col_start", rs.getInt("slot_col_start"));
                row.put("slot_col_end", rs.getInt("slot_col_end"));
                row.put("entry_row", rs.getInt("entry_row"));
                row.put("entry_col", rs.getInt("entry_col"));
                row.put("exit_row", rs.getInt("exit_row"));
                row.put("exit_col", rs.getInt("exit_col"));
                floors.add(row);
            }
        }
        return floors;
    }

    // ── INSERT PARKING FLOOR ──────────────────────────────

    // Inserts a new floor grid configuration.
    // Uses ON CONFLICT DO NOTHING to safely skip duplicate floor entries.
    public void insertParkingFloor(int floorNum,
            int totalRows, int totalCols, int pathWidth,
            int slotColStart, int slotColEnd,
            int entryRow, int entryCol,
            int exitRow, int exitCol)
            throws SQLException {

        String sql = "INSERT INTO parking_floors "
                + "(floor_num, total_rows, total_cols, path_width, "
                + "slot_col_start, slot_col_end, "
                + "entry_row, entry_col, exit_row, exit_col) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (floor_num) DO NOTHING";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floorNum);
            ps.setInt(2, totalRows);
            ps.setInt(3, totalCols);
            ps.setInt(4, pathWidth);
            ps.setInt(5, slotColStart);
            ps.setInt(6, slotColEnd);
            ps.setInt(7, entryRow);
            ps.setInt(8, entryCol);
            ps.setInt(9, exitRow);
            ps.setInt(10, exitCol);
            ps.executeUpdate();
        }
    }

    // ── SPLIT SLOT ────────────────────────────────────────

    // Deletes the parent slot and inserts child slots atomically.
    // Child slot numbers start from MAX(slot_num)+1 on the floor
    // after the parent is deleted, to avoid numbering conflicts.
    // Returns a list of newly created child slot IDs.
    private int getNextSlotNum(Connection con, int floorNum) throws SQLException {
        String sql = "SELECT COALESCE(MAX(slot_num), 0) + 1 FROM parking_slots WHERE floor_num = ?";
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, floorNum);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getInt(1);
            }
        }
        return 1;
    }

    public List<Integer> splitSlot(
            int parentId,
            int parentFloor,
            int parentSlotNum,
            int parentSlotRow,
            int parentColStart,
            String newType,
            int newSize,
            int childCount,
            int allowedMins,
            double rate,
            double penalty)
            throws SQLException {

        String deleteSql = "DELETE FROM parking_slots WHERE id = ?";

        String insertSql = "INSERT INTO parking_slots "
                + "(floor_num, slot_num, vehicle_type, "
                + "allowed_mins, rate_per_hr, penalty_per_hr, "
                + "slot_row, col_start, col_end, size) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?) "
                + "RETURNING id";

        List<Integer> newIds = new ArrayList<>();

        try (Connection con = pool.getConnection()) {
            con.setAutoCommit(false);
            try {
                // Delete the parent slot.
                try (PreparedStatement ps = con.prepareStatement(deleteSql)) {
                    ps.setInt(1, parentId);
                    ps.executeUpdate();
                }

                // Get next slot number after parent is removed,
                // so child slot numbers don't conflict.
                int baseSlotNum = getNextSlotNum(con, parentFloor);

                // Insert each child slot with its grid position.
                for (int i = 0; i < childCount; i++) {
                    int childColStart = parentColStart + (i * newSize);
                    int childColEnd = childColStart + newSize - 1;
                    int childSlotNum = baseSlotNum + i;

                    try (PreparedStatement ps = con.prepareStatement(insertSql)) {
                        ps.setInt(1, parentFloor);
                        ps.setInt(2, childSlotNum);
                        ps.setString(3, newType);
                        ps.setInt(4, allowedMins);
                        ps.setDouble(5, rate);
                        ps.setDouble(6, penalty);
                        ps.setInt(7, parentSlotRow);
                        ps.setInt(8, childColStart);
                        ps.setInt(9, childColEnd);
                        ps.setInt(10, newSize);

                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                newIds.add(rs.getInt(1));
                            }
                        }
                    }
                }

                con.commit();
                return newIds;

            } catch (SQLException e) {
                con.rollback();
                throw e;
            } finally {
                con.setAutoCommit(true);
            }
        }
    }

    // ── COMBINE SLOTS ─────────────────────────────────────

    // Deletes all child slots and inserts one merged slot atomically.
    // The merged slot's slot number is the smallest child slot number.
    // Returns the new merged slot ID.
    public int combineSlots(
            List<Integer> slotIds,
            int floor,
            int slotRow,
            int colStart,
            int colEnd,
            int minSlotNum,
            String newType,
            int newSize,
            int allowedMins,
            double rate,
            double penalty)
            throws SQLException {

        String deleteSql = "DELETE FROM parking_slots WHERE id = ?";

        String insertSql = "INSERT INTO parking_slots "
                + "(floor_num, slot_num, vehicle_type, "
                + "allowed_mins, rate_per_hr, penalty_per_hr, "
                + "slot_row, col_start, col_end, size) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?) "
                + "RETURNING id";

        try (Connection con = pool.getConnection()) {
            con.setAutoCommit(false);
            try {
                // Delete all child slots being combined.
                for (int id : slotIds) {
                    try (PreparedStatement ps = con.prepareStatement(deleteSql)) {
                        ps.setInt(1, id);
                        ps.executeUpdate();
                    }
                }

                // Insert the new merged slot.
                int newId = -1;
                try (PreparedStatement ps = con.prepareStatement(insertSql)) {
                    ps.setInt(1, floor);
                    ps.setInt(2, minSlotNum);
                    ps.setString(3, newType);
                    ps.setInt(4, allowedMins);
                    ps.setDouble(5, rate);
                    ps.setDouble(6, penalty);
                    ps.setInt(7, slotRow);
                    ps.setInt(8, colStart);
                    ps.setInt(9, colEnd);
                    ps.setInt(10, newSize);

                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next())
                            newId = rs.getInt(1);
                    }
                }

                con.commit();
                return newId;

            } catch (SQLException e) {
                con.rollback();
                throw e;
            } finally {
                con.setAutoCommit(true);
            }
        }
    }

    // ── GET SLOT BY ID (with grid columns) ────────────────

    // Returns full slot details including grid position.
    // Used before split/combine to load parent slot data.
    public Slot getSlotById(int slotId) throws SQLException {

        String sql = "SELECT id, floor_num, slot_num, vehicle_type, "
                + "allowed_mins, rate_per_hr, penalty_per_hr, "
                + "COALESCE(slot_row,  -1) AS slot_row, "
                + "COALESCE(col_start, -1) AS col_start, "
                + "COALESCE(col_end,   -1) AS col_end, "
                + "COALESCE(size,      -1) AS size "
                + "FROM parking_slots WHERE id = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, slotId);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Slot(
                            rs.getInt("id"),
                            rs.getInt("floor_num"),
                            rs.getInt("slot_num"),
                            rs.getString("vehicle_type"),
                            rs.getInt("allowed_mins"),
                            rs.getDouble("rate_per_hr"),
                            rs.getDouble("penalty_per_hr"),
                            rs.getInt("slot_row"),
                            rs.getInt("col_start"),
                            rs.getInt("col_end"),
                            rs.getInt("size"));
                }
            }
        }
        return null;
    }

    public List<Integer> getOccupiedSlots(int floor) {
        List<Integer> list = new ArrayList<>();
        String sql = "SELECT slot_id FROM active_parking WHERE floor_num=?";
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                list.add(rs.getInt("slot_id"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    // ── LAYOUT REMOVAL ───────────────────────────────────

    /**
     * Resolve slot IDs for a rectangular block removal.
     * Returns only active slots fully within the region.
     */
    public List<Integer> resolveBlockSlotIds(int floor, int rowStart, int rowEnd,
            int colStart, int colEnd) throws Exception {
        List<Integer> ids = new ArrayList<>();
        String sql = """
            SELECT id FROM parking_slots
            WHERE floor_num = ?
              AND is_active = true
              AND slot_row BETWEEN ? AND ?
              AND col_start >= ?
              AND col_end <= ?
            ORDER BY id
        """;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ps.setInt(2, rowStart);
            ps.setInt(3, rowEnd);
            ps.setInt(4, colStart);
            ps.setInt(5, colEnd);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) ids.add(rs.getInt("id"));
        }
        return ids;
    }

    /**
     * Detect slots that partially overlap the removal region boundary.
     * These must be blocked to avoid half-slots remaining in the grid.
     */
    public List<Integer> detectPartialOverlap(int floor, int rowStart, int rowEnd,
            int colStart, int colEnd) throws Exception {
        List<Integer> ids = new ArrayList<>();
        String sql = """
            SELECT id FROM parking_slots
            WHERE floor_num = ?
              AND is_active = true
              AND slot_row BETWEEN ? AND ?
              AND (
                (col_start < ? AND col_end >= ?)
                OR (col_start <= ? AND col_end > ?)
              )
            ORDER BY id
        """;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ps.setInt(2, rowStart);
            ps.setInt(3, rowEnd);
            ps.setInt(4, colStart);
            ps.setInt(5, colStart);
            ps.setInt(6, colEnd);
            ps.setInt(7, colEnd);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) ids.add(rs.getInt("id"));
        }
        return ids;
    }

    /**
     * Check if any of the given slot IDs have active vehicles parked.
     * Returns map of slot_id -> number_plate for conflicting slots.
     */
    public Map<Integer, String> checkActiveVehiclesInSlots(List<Integer> slotIds) throws Exception {
        Map<Integer, String> conflicts = new java.util.LinkedHashMap<>();
        if (slotIds == null || slotIds.isEmpty()) return conflicts;
        String sql = """
            SELECT ap.slot_id, ap.number_plate
            FROM active_parking ap
            WHERE ap.slot_id = ANY(?)
        """;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            Array arr = conn.createArrayOf("integer",
                slotIds.stream().map(Integer::intValue).toArray());
            ps.setArray(1, arr);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                conflicts.put(rs.getInt("slot_id"), rs.getString("number_plate"));
            }
        }
        return conflicts;
    }

    /**
     * Validate that all given slot IDs exist and are active on the given floor.
     * Returns list of IDs that are invalid (not found or already inactive).
     */
    public List<Integer> validateSlotIds(int floor, List<Integer> slotIds) throws Exception {
        List<Integer> invalid = new ArrayList<>();
        if (slotIds == null || slotIds.isEmpty()) return invalid;
        String sql = """
            SELECT id FROM parking_slots
            WHERE floor_num = ? AND is_active = true AND id = ANY(?)
        """;
        Set<Integer> found = new java.util.HashSet<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            Array arr = conn.createArrayOf("integer",
                slotIds.stream().map(Integer::intValue).toArray());
            ps.setArray(2, arr);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) found.add(rs.getInt("id"));
        }
        for (int id : slotIds) {
            if (!found.contains(id)) invalid.add(id);
        }
        return invalid;
    }

    /**
     * Execute the removal transaction:
     * 1. Soft-delete slots (is_active = false)
     * 2. Write audit record to removed_regions
     * 3. Bump layout_version on parking_floors
     * Returns the new removed_regions id.
     */
    public int executeRemoval(int floor, Integer rowStart, Integer rowEnd,
            Integer colStart, Integer colEnd,
            List<Integer> slotIds, String reason, String removedBy) throws Exception {

        String deactivateSql = """
            UPDATE parking_slots SET is_active = false WHERE id = ANY(?)
        """;
        String auditSql = """
            INSERT INTO removed_regions
              (floor_num, row_start, row_end, col_start, col_end,
               slot_ids, reason, removed_by, layout_version_at_removal)
            VALUES (?,?,?,?,?, ?,?,?,
              (SELECT layout_version FROM parking_floors WHERE floor_num = ?))
            RETURNING id
        """;
        String versionSql = """
            UPDATE parking_floors
            SET layout_version = layout_version + 1,
                version_updated_at = NOW()
            WHERE floor_num = ?
        """;

        try (Connection conn = pool.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // advisory lock for this floor
                try (PreparedStatement lock = conn.prepareStatement(
                        "SELECT pg_advisory_xact_lock(?)")) {
                    lock.setInt(1, floor);
                    lock.execute();
                }

                // deactivate slots
                Array arr = conn.createArrayOf("integer",
                    slotIds.stream().map(Integer::intValue).toArray());
                try (PreparedStatement ps = conn.prepareStatement(deactivateSql)) {
                    ps.setArray(1, arr);
                    ps.executeUpdate();
                }

                // audit record
                int regionId;
                try (PreparedStatement ps = conn.prepareStatement(auditSql)) {
                    ps.setInt(1, floor);
                    ps.setObject(2, rowStart, java.sql.Types.INTEGER);
                    ps.setObject(3, rowEnd, java.sql.Types.INTEGER);
                    ps.setObject(4, colStart, java.sql.Types.INTEGER);
                    ps.setObject(5, colEnd, java.sql.Types.INTEGER);
                    ps.setArray(6, arr);
                    ps.setString(7, reason);
                    ps.setString(8, removedBy);
                    ps.setInt(9, floor);
                    ResultSet rs = ps.executeQuery();
                    rs.next();
                    regionId = rs.getInt(1);
                }

                // bump version
                try (PreparedStatement ps = conn.prepareStatement(versionSql)) {
                    ps.setInt(1, floor);
                    ps.executeUpdate();
                }

                conn.commit();
                return regionId;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    /**
     * Get current layout version for a floor.
     */
    public int getLayoutVersion(int floor) throws Exception {
        String sql = "SELECT layout_version FROM parking_floors WHERE floor_num = ?";
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) return rs.getInt(1);
        }
        return 1;
    }

    /**
     * Get removal history for a floor.
     */
    public List<Map<String, Object>> getRemovalHistory(int floor) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        String sql = """
            SELECT id, floor_num, row_start, row_end, col_start, col_end,
                   slot_ids, reason, removed_by, removed_at, layout_version_at_removal
            FROM removed_regions
            WHERE floor_num = ?
            ORDER BY removed_at DESC
        """;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> row = new java.util.LinkedHashMap<>();
                row.put("id", rs.getInt("id"));
                row.put("floor_num", rs.getInt("floor_num"));
                row.put("row_start", rs.getObject("row_start"));
                row.put("row_end", rs.getObject("row_end"));
                row.put("col_start", rs.getObject("col_start"));
                row.put("col_end", rs.getObject("col_end"));
                Array arr = rs.getArray("slot_ids");
                if (arr != null) {
                    Object[] raw = (Object[]) arr.getArray();
                    java.util.List<Integer> ids = new java.util.ArrayList<>();
                    for (Object o : raw) ids.add(((Number)o).intValue());
                    row.put("slot_ids", ids);
                } else {
                    row.put("slot_ids", null);
                }
                row.put("reason", rs.getString("reason"));
                row.put("removed_by", rs.getString("removed_by"));
                row.put("removed_at", rs.getTimestamp("removed_at") != null
                    ? rs.getTimestamp("removed_at").toLocalDateTime().toString() : null);
                row.put("layout_version_at_removal", rs.getInt("layout_version_at_removal"));
                list.add(row);
            }
        }
        return list;
    }


    // ── LAYOUT REMOVAL ───────────────────────────────────



    // LAYOUT ROLLBACK

    public List<Integer> resolveRemovedBlockSlots(int floor, int rowStart, int rowEnd,
            int colStart, int colEnd) throws Exception {
        List<Integer> ids = new ArrayList<>();
        String sql = """
            SELECT id FROM parking_slots
            WHERE floor_num = ?
              AND is_active = false
              AND slot_row BETWEEN ? AND ?
              AND col_start >= ?
              AND col_end <= ?
            ORDER BY id
        """;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor); ps.setInt(2, rowStart); ps.setInt(3, rowEnd);
            ps.setInt(4, colStart); ps.setInt(5, colEnd);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) ids.add(rs.getInt("id"));
        }
        return ids;
    }

    public List<Integer> validateRemovedSlotIds(int floor, List<Integer> slotIds) throws Exception {
        List<Integer> invalid = new ArrayList<>();
        if (slotIds == null || slotIds.isEmpty()) return invalid;
        String sql = "SELECT id FROM parking_slots WHERE floor_num = ? AND is_active = false AND id = ANY(?)";
        Set<Integer> found = new java.util.HashSet<>();
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            Array arr = conn.createArrayOf("integer", slotIds.stream().map(Integer::intValue).toArray());
            ps.setArray(2, arr);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) found.add(rs.getInt("id"));
        }
        for (int id : slotIds) { if (!found.contains(id)) invalid.add(id); }
        return invalid;
    }

    public List<Map<String, Object>> getRemovedSlotsByFloor(int floor) throws Exception {
        List<Map<String, Object>> list = new ArrayList<>();
        String sql = """
            SELECT id, floor_num, slot_num, vehicle_type, slot_row, col_start, col_end, size
            FROM parking_slots
            WHERE floor_num = ? AND is_active = false
              AND slot_row IS NOT NULL AND col_start IS NOT NULL
            ORDER BY slot_row, col_start
        """;
        try (Connection conn = pool.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, floor);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> r = new LinkedHashMap<>();
                r.put("slot_id", rs.getInt("id"));
                r.put("floor_num", rs.getInt("floor_num"));
                r.put("slot_num", rs.getInt("slot_num"));
                r.put("vehicle_type", rs.getString("vehicle_type"));
                r.put("slot_row", rs.getInt("slot_row"));
                r.put("col_start", rs.getInt("col_start"));
                r.put("col_end", rs.getInt("col_end"));
                r.put("size", rs.getInt("size"));
                list.add(r);
            }
        }
        return list;
    }

    public int executeRollback(int floor, List<Integer> slotIds, String restoredBy) throws Exception {
        String restoreSql = "UPDATE parking_slots SET is_active = true WHERE id = ANY(?)";
        String auditSql = """
            INSERT INTO removed_regions
              (floor_num, slot_ids, reason, removed_by, layout_version_at_removal)
            VALUES (?, ?, ?, ?,
              (SELECT layout_version FROM parking_floors WHERE floor_num = ?))
            RETURNING id
        """;
        String versionSql = """
            UPDATE parking_floors
            SET layout_version = layout_version + 1, version_updated_at = NOW()
            WHERE floor_num = ?
        """;
        try (Connection conn = pool.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement lock = conn.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
                    lock.setInt(1, floor); lock.execute();
                }
                Array arr = conn.createArrayOf("integer", slotIds.stream().map(Integer::intValue).toArray());
                try (PreparedStatement ps = conn.prepareStatement(restoreSql)) {
                    ps.setArray(1, arr); ps.executeUpdate();
                }
                int regionId;
                try (PreparedStatement ps = conn.prepareStatement(auditSql)) {
                    ps.setInt(1, floor);
                    ps.setArray(2, arr);
                    ps.setString(3, "ROLLBACK: " + slotIds.size() + " slot(s) restored");
                    ps.setString(4, restoredBy);
                    ps.setInt(5, floor);
                    ResultSet rs = ps.executeQuery(); rs.next();
                    regionId = rs.getInt(1);
                }
                try (PreparedStatement ps = conn.prepareStatement(versionSql)) {
                    ps.setInt(1, floor); ps.executeUpdate();
                }
                conn.commit();
                return regionId;
            } catch (Exception e) { conn.rollback(); throw e;
            } finally { conn.setAutoCommit(true); }
        }
    }

}