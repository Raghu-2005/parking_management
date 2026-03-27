package com.parking.dao;

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

    // ── STATS CACHE (change E) ────────────────────────────
    // Key = canonical filter string, Value = [stats map, expiry epoch ms]
    // TTL = 60 seconds. Shared across all admin sessions.
    // Memory: ~200 bytes per cached entry — negligible.
    private final ConcurrentHashMap<String, Object[]> statsCache = new ConcurrentHashMap<>();
    private static final long CACHE_TTL_MS = 60_000L;

    public ParkingDAO(ConnectionPool pool) {
        this.pool = pool;
    }

    // ── STARTUP LOADS ────────────────────────────────────

    public List<Slot> loadAllSlots() throws SQLException {
        // slot_row, col_start, col_end, size are nullable —
        // slots created before migration have them as NULL.
        // COALESCE returns -1 for those rows so the legacy
        // 7-arg Slot constructor path is never needed here;
        // we always use the full 11-arg constructor and let
        // hasGridPosition() tell callers which slots are on
        // the grid.
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

    // ── ACTIVE VEHICLES PAGED (change F) ─────────────────
    // page is 1-based, pageSize = 50.
    // Returns List<Map> so the servlet can build JSON without a slotNum DB call per
    // row.
    // slot_num is joined directly — zero extra queries per vehicle.
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

    // Total count of active vehicles — used for pagination metadata
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

    // ── ACTIVE VEHICLES FILTERED + PAGED ─────────────────
    // floor and type are both optional — pass null to skip that filter.
    // When both are null this behaves identically to getActiveVehiclesPaged.
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

    // Count for filtered active vehicles — drives pagination metadata
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

    // ── FREE SLOT QUERIES ────────────────────────────────
    //
    // ALL slot selection methods use SELECT ... FOR UPDATE SKIP LOCKED
    // inside a transaction that ends with INSERT INTO active_parking.
    //
    // WHY THIS FIXES THE RACE CONDITION:
    // Without this: Thread1 checks slot → free. Thread2 checks same slot → free.
    // Both proceed. Both insert. Same slot occupied by two vehicles. ✗
    //
    // With FOR UPDATE SKIP LOCKED:
    // Thread1 selects slot 5 → PostgreSQL LOCKS row 5 immediately.
    // Thread2 tries slot 5 → it is locked → SKIP LOCKED skips it → picks slot 6.
    // Thread1 inserts into active_parking → commits → lock released.
    // Two threads NEVER get the same slot. ✓
    //
    // The connection must stay open from SELECT through INSERT.
    // We pass the Connection in so the caller controls the transaction.
    //
    public Slot lockNextFreeSlot(Connection con,
            int floor, String type) throws SQLException {

        // LEFT JOIN active_parking to find slots NOT currently occupied
        // FOR UPDATE SKIP LOCKED — locks the chosen row atomically,
        // skips any row already locked by another concurrent transaction
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

    // ── ATOMIC ENTRY — lock slot + insert active_parking in one transaction ──
    // This is the ONLY correct way to park a vehicle under concurrency.
    // Returns the locked Slot or null if no slot available.
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

                // Insert into active_parking WITHIN THE SAME TRANSACTION
                // The slot row lock is held until this commit
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
                PreparedStatement ps = con.prepareStatement(
                        sql.toString())) {

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

            } catch (SQLException e) {
                con.rollback();
                throw e;
            } finally {
                con.setAutoCommit(true);
            }
        }
    }

    // ── SLOT MANAGEMENT ──────────────────────────────────

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

    public void deleteSlot(int slotId) throws SQLException {
        // Check if any vehicle is currently parked in this slot
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
        // Safe to delete
        String sql = "DELETE FROM parking_slots WHERE id = ?";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, slotId);
            ps.executeUpdate();
        }
    }

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

    public List<Map<String, Object>> getSlotsByFloor(int floor)
            throws SQLException {

        String sql = "SELECT ps.id, ps.slot_num, ps.vehicle_type, "
                + "ps.allowed_mins, ps.rate_per_hr, ps.penalty_per_hr, "
                + "ap.number_plate "
                + "FROM parking_slots ps "
                + "LEFT JOIN active_parking ap ON ps.id = ap.slot_id "
                + "WHERE ps.floor_num = ? "
                + "ORDER BY ps.slot_num";

        List<Map<String, Object>> slots = new ArrayList<>();

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> slot = new HashMap<>();
                    slot.put("slot_id", rs.getInt("id"));
                    slot.put("slot_num", rs.getInt("slot_num"));
                    slot.put("vehicle_type", rs.getString("vehicle_type"));
                    slot.put("allowed_mins", rs.getInt("allowed_mins"));
                    slot.put("rate_per_hr", rs.getDouble("rate_per_hr"));
                    slot.put("penalty_per_hr", rs.getDouble("penalty_per_hr"));
                    String plate = rs.getString("number_plate");
                    slot.put("status", plate != null ? "OCCUPIED" : "FREE");
                    slot.put("number_plate", plate);
                    slots.add(slot);
                }
            }
        }
        return slots;
    }

    // ── FLOOR MANAGEMENT ─────────────────────────────────

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

    public void unblockFloor(int floor) throws SQLException {
        String sql = "DELETE FROM blocked_floor WHERE floor_num = ?";

        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {

            ps.setInt(1, floor);
            ps.executeUpdate();
        }
    }

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
    // Fully dynamic — no hardcoded list anywhere.
    // Used by getAllAvailability so new types (bus, van, etc.)
    // appear automatically the moment a slot is created for them.
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
    // Returns all distinct allowed_mins values from parking_history.
    // Used to populate the allowed_mins dropdown in history filters.
    // Queried from history (not slots) so deleted slots still appear
    // in historical data correctly.
    // ── DISTINCT ALLOWED MINS ─────────────────────────────
    // Returns all distinct allowed_mins values from parking_history.
    // Used to populate the allowed_mins dropdown in history filters.
    // Queried from history (not slots) so deleted slots still appear
    // in historical data correctly.
    // ── DISTINCT ALLOWED MINS ─────────────────────────────
    // Returns distinct allowed_mins values from parking_history.
    // Uses BRIN index on exit_date by filtering last 30 days only.
    // LIMIT 20 stops after enough values for the dropdown.
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

    // page is 1-based. pageSize = 50. Never loads all rows — safe on 10M+ table.
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

    // ── GET SLOT BY SLOT NUMBER — combination 3 ──────────
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
                PreparedStatement ps = con.prepareStatement(
                        sql.toString())) {

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

    // ── GET FREE SLOT NUMBERS — availability ─────────────
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
                PreparedStatement ps = con.prepareStatement(
                        sql.toString())) {

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

    // ── GET ANY FREE SLOT — combination 4 ────────────────
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
                PreparedStatement ps = con.prepareStatement(
                        sql.toString())) {

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

    // ── REGISTER ─────────────────────────────────────────
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

    // ── GET PENDING USERS ─────────────────────────────────
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

    // ── APPROVE USER ──────────────────────────────────────
    public void approveUser(int userId) throws SQLException {
        String sql = "UPDATE users SET status = 'active' WHERE id = ?";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, userId);
            ps.executeUpdate();
        }
    }

    // ── REJECT USER ───────────────────────────────────────
    public void rejectUser(int userId) throws SQLException {
        String sql = "DELETE FROM users WHERE id = ?";
        try (Connection con = pool.getConnection();
                PreparedStatement ps = con.prepareStatement(sql)) {
            ps.setInt(1, userId);
            ps.executeUpdate();
        }
    }

    // ── CAN APPROVE ADMIN ─────────────────────────────────
    // Any currently active admin can approve new admin registrations.
    // The first admin (id=1) bootstraps the system — once more admins
    // are active, all of them have equal approval power.
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

    // ── GET ALL USERS ─────────────────────────────────────
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

    // ── FILTERED HISTORY (backward compat) ───────────────
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

    // ── FILTERED HISTORY PAGE ─────────────────────────────────────────
    //
    // Single query — BRIN on exit_date prunes the table to the date window,
    // then LIMIT/OFFSET operates on that small bounded set.
    //
    // WHY OFFSET IS SAFE HERE:
    // Without BRIN: OFFSET 300 on 10M rows = read and discard 300 random pages.
    // Slow.
    // With BRIN: BRIN narrows to ~2000 rows in date window.
    // OFFSET 300 within 2000 rows = sequential reads from cached pages. Fast.
    // Default 30-day window ensures BRIN always activates.
    //
    // Memory at all times = exactly pageSize rows (50 rows).
    // Direct page jump: page 7 = OFFSET (7-1)×50 = 300. No stored IDs. No previous
    // data.
    //
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

        int offset = (page - 1) * pageSize;

        StringBuilder sql = new StringBuilder(
                "SELECT id, number_plate, slot_id, slot_num, floor_num, "
                        + "vehicle_type, entry_time, exit_time, "
                        + "allowed_mins, total_mins, extra_mins, "
                        + "total_amount, penalty_amount "
                        + "FROM parking_history WHERE 1=1");

        List<Object> params = new ArrayList<>();

        // DATE RANGE FIRST — BRIN index on exit_date prunes irrelevant disk blocks
        if (from != null) {
            sql.append(" AND exit_date >= ?");
            params.add(java.sql.Date.valueOf(from.toLocalDate()));
        }
        if (to != null) {
            sql.append(" AND exit_date <= ?");
            params.add(java.sql.Date.valueOf(to.toLocalDate()));
        }
        // OPTIONAL FILTERS — applied on already-pruned window
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

        // LIMIT + OFFSET — safe because BRIN already narrowed the row set
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

    // ── FILTERED HISTORY STATS ────────────────────────────────────────
    //
    // Runs a single COUNT + SUM query for total_count, total_revenue,
    // total_penalty.
    // Results are cached for 60 seconds per unique filter combination.
    // Multiple admins searching the same filter hit the cache — zero extra DB
    // calls.
    //
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

        // ── BUILD CACHE KEY from all filter values ────────────────────
        String cacheKey = floor + "|" + slot + "|" + vehicleType + "|"
                + plate + "|" + from + "|" + to + "|" + penaltyOnly + "|"
                + minPenalty + "|" + maxPenalty + "|" + minAmount + "|" + maxAmount
                + "|" + allowedMins + "|" + minTotalMins + "|" + maxTotalMins
                + "|" + minExtraMins;

        // ── CHECK CACHE ───────────────────────────────────────────────
        Object[] cached = statsCache.get(cacheKey);
        if (cached != null) {
            long expiry = (long) cached[1];
            if (System.currentTimeMillis() < expiry) {
                return (Map<String, Object>) cached[0];
            }
            statsCache.remove(cacheKey);
        }

        // ── CACHE MISS — run the DB query ─────────────────────────────
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

        // ── STORE IN CACHE with expiry timestamp ──────────────────────
        statsCache.put(cacheKey, new Object[] { stats,
                System.currentTimeMillis() + CACHE_TTL_MS });

        return stats;
    }

    // ── FLOOR REPORT ─────────────────────────────────────
    //
    // Returns per-vehicle-type breakdown for a floor:
    // vehicle_type, total_vehicles, total_revenue, total_penalty,
    // avg_duration_mins, max_duration_mins
    // Plus a grand-total row with vehicle_type = "ALL".
    //
    // Date range is optional — when provided, hits idx_ph_exit_date_brin
    // so the scan is bounded even on 10M+ rows.
    // When no date range is given, PostgreSQL uses idx_history_main
    // (floor_num is indexed there) — still fast for per-floor aggregation.
    //
    public List<Map<String, Object>> getFloorReport(
            int floor,
            LocalDateTime from,
            LocalDateTime to) throws SQLException {

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

        // date range — uses BRIN index on exit_date when provided
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

        // totals accumulated for the ALL summary row
        long grandVehicles = 0;
        double grandRevenue = 0;
        double grandPenalty = 0;
        double grandAvgSum = 0; // sum of per-type averages (weighted below)
        long grandMax = 0;
        int typeCount = 0;

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
                    grandAvgSum += avg * tv; // weighted for correct grand avg
                    if (mx > grandMax)
                        grandMax = mx;
                    typeCount++;
                }
            }
        }

        // grand total summary row
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

        return rows;
    }

    // ── LOAD VEHICLE SIZES ────────────────────────────────
    // Called once at startup by AppInitializer.
    // Returns all rows from vehicle_size table.
    // Result is loaded into ParkingStore.vehicleSizes map.
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

    // ── LOAD PARKING FLOORS ───────────────────────────────
    // Called once at startup by AppInitializer.
    // Returns all rows from parking_floors table as maps.
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
    // Called by admin when creating a new floor with grid
    // dimensions. Inserts one row into parking_floors.
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
    // Deletes the parent slot and inserts child slots in one
    // atomic transaction. Returns the list of new slot ids.
    //
    // parentId : slot being split
    // newType : vehicle type for child slots
    // newSize : size of newType (from vehicle_size)
    // childCount : parent.size / newSize
    // parentSlotRow, parentColStart : grid position of parent
    // allowedMins, rate, penalty : billing config for children
    //
    // slotNum for children: parent.slotNum * 100 + i
    // This keeps slotNum unique and human-readable while
    // making it clear which parent they came from.

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
                // delete parent
                try (PreparedStatement ps = con.prepareStatement(deleteSql)) {
                    ps.setInt(1, parentId);
                    ps.executeUpdate();
                }

                // get base slot num ONCE after parent is deleted
                // so MAX(slot_num) does not include the parent anymore
                int baseSlotNum = getNextSlotNum(con, parentFloor);

                // insert children
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
    // Deletes all child slots and inserts one merged slot in
    // one atomic transaction. Returns the new slot id.
    //
    // slotIds : ids of slots being combined (sorted by
    // col_start before calling this method)
    // newType : vehicle type for merged slot
    // newSize : total size (sum of children sizes)
    // floor, slotRow, colStart, colEnd : grid position
    // allowedMins, rate, penalty : billing config
    //
    // slotNum for merged slot: min(childSlotNums)
    // Simple and consistent — smallest child num becomes
    // the merged slot's display number.
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
                // delete all children
                for (int id : slotIds) {
                    try (PreparedStatement ps = con.prepareStatement(deleteSql)) {
                        ps.setInt(1, id);
                        ps.executeUpdate();
                    }
                }

                // insert merged slot
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

    // ── GET SLOT BY ID (full, with grid columns) ──────────
    // Used by split/combine service methods to load parent
    // slot details including grid position before operating.
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

}