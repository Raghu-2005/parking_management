package com.parking.service;

import com.parking.dao.ParkingDAO;
import com.parking.model.ActiveVehicle;
import com.parking.model.BillResult;
import com.parking.model.Slot;
import com.parking.store.ParkingStore;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParkingService {

    private final ParkingDAO dao;
    private final ParkingStore store;

    public ParkingService(ParkingDAO dao, ParkingStore store) {
        this.dao = dao;
        this.store = store;
    }

    // ── VEHICLE ENTRY ────────────────────────────────────

    public Slot vehicleEntry(String plate,
            Integer floor,
            Integer slotNum,
            String vehicleType)
            throws ParkingException {

        // validate required params
        if (plate == null || plate.isBlank()) {
            throw new ParkingException(
                    "INVALID_PARAMS",
                    "number_plate is required");
        }
        if (vehicleType == null || vehicleType.isBlank()) {
            throw new ParkingException(
                    "INVALID_PARAMS",
                    "vehicle_type is required");
        }

        // check floor blocked — O(1) in memory
        if (floor != null && store.isFloorBlocked(floor)) {
            String reason = store.getBlockReason(floor);
            throw new ParkingException(
                    "FLOOR_BLOCKED",
                    "Floor " + floor + " is blocked. Reason: " + reason);
        }

        // check duplicate vehicle — O(1) in memory
        if (store.isVehicleParked(plate)) {
            throw new ParkingException(
                    "VEHICLE_ALREADY_PARKED",
                    "Vehicle " + plate + " is already parked");
        }

        try {
            // ── ATOMIC SLOT SELECTION + INSERTION ────────────────────
            // Uses SELECT FOR UPDATE SKIP LOCKED inside a single transaction.
            // No two threads can ever get the same slot — the DB lock
            // guarantees this at the PostgreSQL level, not just in memory.
            Slot slot = dao.atomicVehicleEntry(
                    plate, floor, slotNum, vehicleType,
                    store.getAllBlockedFloors());

            if (slot == null) {
                String msg = (floor != null && slotNum != null)
                        ? "Slot " + slotNum + " on floor " + floor + " is occupied or not found"
                        : (floor != null)
                                ? "No " + vehicleType + " slots available on floor " + floor
                                : "No " + vehicleType + " slots available anywhere";
                throw new ParkingException("SLOT_NOT_FOUND", msg);
            }

            // ── UPDATE IN-MEMORY STORE after DB success ───────────────
            // DB is the source of truth. Memory store is updated AFTER
            // the DB transaction commits successfully.
            LocalDateTime entryTime = LocalDateTime.now();
            ActiveVehicle vehicle = new ActiveVehicle(
                    plate, slot.id, slot.floor, vehicleType, entryTime);

            store.addOccupied(slot.floor, vehicleType, slot.id, slot);

            boolean added = store.addActiveVehicle(vehicle);
            if (!added) {
                // Very rare — means plate was added by another thread
                // between our isVehicleParked check and here.
                // DB already has the record — rollback memory is not needed
                // because DB transaction already committed.
                throw new ParkingException(
                        "VEHICLE_ALREADY_PARKED",
                        "Vehicle " + plate + " is already parked");
            }

            return slot;

        } catch (java.sql.SQLException e) {
            // Unique constraint on number_plate in active_parking
            // means duplicate plate — caught here as a safety net
            if (e.getMessage() != null &&
                    e.getMessage().contains("duplicate key")) {
                throw new ParkingException(
                        "VEHICLE_ALREADY_PARKED",
                        "Vehicle " + plate + " is already parked");
            }
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    // ── VEHICLE EXIT ─────────────────────────────────────

    public BillResult vehicleExit(String plate)
            throws ParkingException {

        if (plate == null || plate.isBlank()) {
            throw new ParkingException(
                    "INVALID_PARAMS",
                    "number_plate is required");
        }

        // get vehicle from memory — O(1)
        ActiveVehicle vehicle = store.getActiveVehicle(plate);

        if (vehicle == null) {
            throw new ParkingException(
                    "VEHICLE_NOT_FOUND",
                    "Vehicle " + plate + " is not currently parked");
        }

        try {
            // get slot from occupiedSlots map — O(1)
            Slot slot = store.getOccupiedSlot(
                    vehicle.floor, vehicle.vehicleType, vehicle.slotId);

            // fallback to DB if not in memory
            if (slot == null) {
                slot = dao.getSlotDetails(vehicle.slotId);
            }

            if (slot == null) {
                throw new ParkingException(
                        "SLOT_NOT_FOUND",
                        "Slot details not found for slot id "
                                + vehicle.slotId);
            }

            // calculate bill
            BillResult bill = calculateBill(vehicle, slot);

            // process exit in DB — transaction
            dao.processExit(bill);

            // update memory after DB success
            store.removeOccupied(
                    vehicle.floor, vehicle.vehicleType, vehicle.slotId);
            store.removeActiveVehicle(plate);

            return bill;

        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    // ── CALCULATE BILL ───────────────────────────────────

    private BillResult calculateBill(ActiveVehicle vehicle,
            Slot slot) {

        LocalDateTime exitTime = LocalDateTime.now();

        long totalMins = ChronoUnit.MINUTES.between(
                vehicle.entryTime, exitTime);

        long extraMins = Math.max(0, totalMins - slot.allowedMins);

        double baseAmount = (totalMins / 60.0) * slot.ratePerHr;
        double penaltyAmount = (extraMins / 60.0) * slot.penaltyPerHr;
        double totalAmount = baseAmount + penaltyAmount;

        // round to 2 decimal places
        totalAmount = Math.round(totalAmount * 100.0) / 100.0;
        penaltyAmount = Math.round(penaltyAmount * 100.0) / 100.0;

        return new BillResult(
                vehicle.numberPlate,
                vehicle.floor,
                slot.id,
                slot.slotNum,
                vehicle.vehicleType,
                vehicle.entryTime,
                exitTime,
                slot.allowedMins,
                totalMins,
                extraMins,
                totalAmount,
                penaltyAmount);
    }

    // ── SLOT MANAGEMENT ──────────────────────────────────

    public int createSlot(int floor,
            int slotNum,
            String type,
            int allowedMins,
            double rate,
            double penalty)
            throws ParkingException {

        try {
            if (dao.slotExists(floor, slotNum, type)) {
                throw new ParkingException(
                        "SLOT_ALREADY_EXISTS",
                        "Slot " + slotNum + " already exists on floor "
                                + floor + " for " + type);
            }

            int slotId = dao.insertSlot(
                    floor, slotNum, type, allowedMins, rate, penalty);

            store.initFloorType(floor, type);

            return slotId;

        } catch (ParkingException e) {
            throw e;
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    public void deleteSlot(int slotId) throws ParkingException {
        try {
            Slot slot = dao.getSlotDetails(slotId);
            if (slot == null) {
                throw new ParkingException(
                        "SLOT_NOT_FOUND",
                        "Slot with id " + slotId + " does not exist");
            }

            if (store.isOccupied(
                    slot.floor, slot.vehicleType, slotId)) {
                throw new ParkingException(
                        "SLOT_OCCUPIED",
                        "Cannot delete — vehicle currently parked in slot "
                                + slotId);
            }

            dao.deleteSlot(slotId);

        } catch (ParkingException e) {
            throw e;
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    public void updateSlot(int slotId,
            int allowedMins,
            double rate,
            double penalty)
            throws ParkingException {

        try {
            Slot slot = dao.getSlotDetails(slotId);
            if (slot == null) {
                throw new ParkingException(
                        "SLOT_NOT_FOUND",
                        "Slot with id " + slotId + " does not exist");
            }

            dao.updateSlot(slotId, allowedMins, rate, penalty);

            // update in memory if currently occupied
            Slot updated = new Slot(
                    slot.id, slot.floor, slot.slotNum,
                    slot.vehicleType, allowedMins, rate, penalty);
            store.updateOccupiedSlot(
                    slot.floor, slot.vehicleType, slotId, updated);

        } catch (ParkingException e) {
            throw e;
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> getSlotsByFloor(int floor)
            throws ParkingException {

        try {
            return dao.getSlotsByFloor(floor);
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    // ── FLOOR MANAGEMENT ─────────────────────────────────

    public void createFloor(int floor) throws ParkingException {
        if (store.floorExists(floor)) {
            throw new ParkingException(
                    "FLOOR_ALREADY_EXISTS",
                    "Floor " + floor + " already exists");
        }
        store.initFloor(floor);
    }

    public void blockFloor(int floor, String reason)
            throws ParkingException {

        try {
            if (!store.floorExists(floor)) {
                throw new ParkingException(
                        "FLOOR_NOT_FOUND",
                        "Floor " + floor + " does not exist");
            }

            if (store.isFloorBlocked(floor)) {
                throw new ParkingException(
                        "FLOOR_ALREADY_BLOCKED",
                        "Floor " + floor + " is already blocked");
            }

            dao.blockFloor(floor, reason);
            store.blockFloor(floor, reason);

        } catch (ParkingException e) {
            throw e;
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    public void unblockFloor(int floor) throws ParkingException {
        try {
            if (!store.floorExists(floor)) {
                throw new ParkingException(
                        "FLOOR_NOT_FOUND",
                        "Floor " + floor + " does not exist");
            }

            if (!store.isFloorBlocked(floor)) {
                throw new ParkingException(
                        "FLOOR_NOT_BLOCKED",
                        "Floor " + floor + " is not blocked");
            }

            dao.unblockFloor(floor);
            store.unblockFloor(floor);

        } catch (ParkingException e) {
            throw e;
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> getAllFloors()
            throws ParkingException {

        try {
            List<Integer> floorNums = dao.getAllFloorNums();
            List<Map<String, Object>> result = new ArrayList<>();

            for (int floor : floorNums) {
                Map<String, Object> f = new HashMap<>();
                int total = dao.getTotalSlotCountByFloor(floor);
                int occupied = store.getOccupiedCount(floor);
                int free = total - occupied;

                f.put("floor", floor);
                f.put("total_slots", total);
                f.put("occupied", occupied);
                f.put("free", free);

                if (store.isFloorBlocked(floor)) {
                    f.put("status", "BLOCKED");
                    f.put("reason", store.getBlockReason(floor));
                } else {
                    f.put("status", "ACTIVE");
                }

                result.add(f);
            }

            return result;

        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    // ── AVAILABILITY ─────────────────────────────────────

    public Map<String, Object> getAvailability(int floor, String type)
            throws ParkingException {

        try {
            int total = dao.getTotalSlotCount(floor, type);
            int occupied = store.getOccupiedCountByType(floor, type);
            int free = total - occupied;

            Set<Integer> occupiedIds = store.getOccupiedIds(floor, type);
            List<Integer> freeSlots = dao.getFreeSlotNums(floor, type, occupiedIds);

            Map<String, Object> result = new HashMap<>();
            result.put("floor", floor);
            result.put("vehicle_type", type);
            result.put("free_count", free);
            result.put("free_slots", freeSlots);

            return result;

        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    public List<Map<String, Object>> getAllAvailability()
            throws ParkingException {

        try {
            List<Map<String, Object>> result = new ArrayList<>();
            List<Integer> floors = dao.getAllFloorNums();

            // fully dynamic — picks up any type the admin has created slots for
            // (car, bike, truck, bus, van, auto, tractor, etc.)
            List<String> types = dao.getDistinctVehicleTypes();

            for (int floor : floors) {
                for (String type : types) {
                    int total = dao.getTotalSlotCount(floor, type);
                    if (total == 0)
                        continue;

                    int occupied = store.getOccupiedCountByType(
                            floor, type);
                    int free = total - occupied;

                    Map<String, Object> row = new HashMap<>();
                    row.put("floor", floor);
                    row.put("vehicle_type", type);
                    row.put("free_count", free);
                    row.put("total", total);
                    result.add(row);
                }
            }

            return result;

        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    // ── VEHICLE DETAILS ──────────────────────────────────

    public ActiveVehicle getVehicleDetails(String plate)
            throws ParkingException {

        ActiveVehicle vehicle = store.getActiveVehicle(plate);

        if (vehicle == null) {
            throw new ParkingException(
                    "VEHICLE_NOT_FOUND",
                    "Vehicle " + plate + " is not currently parked");
        }

        return vehicle;
    }

    // ── HISTORY ──────────────────────────────────────────

    public List<BillResult> getParkingHistory(String plate)
            throws ParkingException {

        try {
            List<BillResult> history = dao.getParkingHistory(plate);

            if (history.isEmpty()) {
                throw new ParkingException(
                        "VEHICLE_NOT_FOUND",
                        "No history found for " + plate);
            }

            return history;

        } catch (ParkingException e) {
            throw e;
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    public List<BillResult> getAllHistory(int page, int pageSize) throws ParkingException {
        try {
            return dao.getAllHistory(page, pageSize);
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    public Collection<ActiveVehicle> getAllActiveVehicles() {
        return store.getAllActiveVehicles();
    }

    // ── SPLIT SLOT ────────────────────────────────────────
    //
    // Splits one slot into (parent.size / newSize) child slots
    // of newType. All children occupy the same slot_row as the
    // parent, subdividing its exact column range.
    //
    // Guards checked before any DB write:
    // 1. Parent slot must exist and have a grid position.
    // 2. Parent must be free — O(1) via activeVehicles map.
    // 3. newType must exist in vehicleSizes — O(1).
    // 4. newSize must be smaller than parent size.
    // 5. parent.size % newSize == 0 (exact subdivision).
    //
    // After DB commit, gridMap and slotMeta are updated.
    // Memory is ONLY updated after successful DB commit.
    //
    public List<Map<String, Object>> splitSlot(
            int parentId,
            String newType,
            int allowedMins,
            double rate,
            double penalty)
            throws ParkingException {

        try {
            // ── Guard 1: parent exists with grid position ─────
            Slot parent = dao.getSlotById(parentId);
            if (parent == null) {
                throw new ParkingException(
                        "SLOT_NOT_FOUND",
                        "Slot " + parentId + " does not exist");
            }
            if (!parent.hasGridPosition()) {
                throw new ParkingException(
                        "NO_GRID_POSITION",
                        "Slot " + parentId
                                + " has no grid position assigned. "
                                + "Assign slot_row and col_start first.");
            }

            // ── Guard 2: parent must be free ──────────────────
            // O(1) — checks activeVehicles map, no DB call
            if (store.isVehicleParkedInSlot(parentId)) {
                throw new ParkingException(
                        "SLOT_OCCUPIED",
                        "Cannot split — a vehicle is currently "
                                + "parked in slot " + parentId);
            }

            // ── Guard 3: newType must be registered ───────────
            int newSize = store.getVehicleSize(newType);
            if (newSize == -1) {
                throw new ParkingException(
                        "INVALID_TYPE",
                        "Vehicle type '" + newType
                                + "' is not registered in vehicle_size table");
            }

            // ── Guard 4: new size must be smaller ─────────────
            if (newSize >= parent.size) {
                throw new ParkingException(
                        "INVALID_SPLIT",
                        "Cannot split: new type '" + newType
                                + "' (size=" + newSize
                                + ") must be smaller than parent size "
                                + parent.size);
            }

            // ── Guard 5: must divide exactly ──────────────────
            if (parent.size % newSize != 0) {
                throw new ParkingException(
                        "INVALID_SPLIT",
                        "Cannot split: parent size " + parent.size
                                + " is not divisible by " + newType
                                + " size " + newSize);
            }

            int childCount = parent.size / newSize;

            // ── DB transaction: delete parent, insert children ─
            List<Integer> newIds = dao.splitSlot(
                    parentId,
                    parent.floor,
                    parent.slotNum,
                    parent.slotRow,
                    parent.colStart,
                    newType,
                    newSize,
                    childCount,
                    allowedMins,
                    rate,
                    penalty);

            // ── Update memory after commit ────────────────────
            // 1. Remove parent from gridMap cells
            store.removeSlotFromGrid(
                    parent.floor,
                    parent.slotRow,
                    parent.colStart,
                    parent.colEnd);

            // 2. Remove parent from slotMeta
            store.removeSlotMeta(parentId);

            // 3. Add each child to gridMap and slotMeta
            List<Map<String, Object>> result = new ArrayList<>();
            for (int i = 0; i < newIds.size(); i++) {
                int childId = newIds.get(i);
                int childColStart = parent.colStart
                        + (i * newSize);
                int childColEnd = childColStart + newSize - 1;

                store.addSlotToGrid(
                        parent.floor,
                        parent.slotRow,
                        childColStart,
                        childColEnd,
                        childId);

                store.putSlotMeta(
                        childId,
                        parent.floor,
                        parent.slotRow,
                        childColStart,
                        childColEnd,
                        newSize);

                // also register in occupiedSlots structure
                // so availability counting works immediately
                store.initFloorType(parent.floor, newType);

                Map<String, Object> child = new HashMap<>();
                child.put("slot_id", childId);
                child.put("floor", parent.floor);
                child.put("slot_row", parent.slotRow);
                child.put("col_start", childColStart);
                child.put("col_end", childColEnd);
                child.put("type", newType);
                child.put("size", newSize);
                result.add(child);
            }

            return result;

        } catch (ParkingException e) {
            throw e;
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    // ── COMBINE SLOTS ─────────────────────────────────────
    //
    // Combines 2 or more adjacent slots in the same slot_row
    // into one larger slot of newType.
    //
    // Guards checked before any DB write:
    // 1. At least 2 slots provided.
    // 2. All slots must exist and have grid positions.
    // 3. All slots must be free — O(1) each.
    // 4. All slots must be on the same floor.
    // 5. All slots must be on the same slot_row.
    // 6. Slots must be contiguous (sorted by col_start,
    // col_start[i+1] == col_end[i] + 1).
    // 7. newType must exist in vehicleSizes.
    // 8. sum of sizes must equal newType.size exactly.
    //
    // After DB commit, gridMap and slotMeta are updated.
    // Memory is ONLY updated after successful DB commit.
    //
    public Map<String, Object> combineSlots(
            List<Integer> slotIds,
            String newType,
            int allowedMins,
            double rate,
            double penalty)
            throws ParkingException {

        try {
            // ── Guard 1: at least 2 slots ─────────────────────
            if (slotIds == null || slotIds.size() < 2) {
                throw new ParkingException(
                        "INVALID_COMBINE",
                        "At least 2 slots are required to combine");
            }

            // ── Guard 2: load all slots, verify grid positions ─
            List<Slot> slots = new ArrayList<>();
            for (int id : slotIds) {
                Slot s = dao.getSlotById(id);
                if (s == null) {
                    throw new ParkingException(
                            "SLOT_NOT_FOUND",
                            "Slot " + id + " does not exist");
                }
                if (!s.hasGridPosition()) {
                    throw new ParkingException(
                            "NO_GRID_POSITION",
                            "Slot " + id
                                    + " has no grid position assigned");
                }
                slots.add(s);
            }

            // ── Guard 3: all slots must be free ───────────────
            // O(1) per slot via activeVehicles map
            for (Slot s : slots) {
                if (store.isVehicleParkedInSlot(s.id)) {
                    throw new ParkingException(
                            "SLOT_OCCUPIED",
                            "Cannot combine — a vehicle is parked "
                                    + "in slot " + s.id);
                }
            }

            // ── Guard 4: all same floor ───────────────────────
            int floor = slots.get(0).floor;
            for (Slot s : slots) {
                if (s.floor != floor) {
                    throw new ParkingException(
                            "INVALID_COMBINE",
                            "All slots must be on the same floor. "
                                    + "Slot " + s.id + " is on floor "
                                    + s.floor + ", expected floor " + floor);
                }
            }

            // ── Guard 5: all same slot_row ────────────────────
            // This is the key physical adjacency rule.
            // Slots in different slot_rows are separated by a
            // path lane — they can never be combined.
            int slotRow = slots.get(0).slotRow;
            for (Slot s : slots) {
                if (s.slotRow != slotRow) {
                    throw new ParkingException(
                            "NOT_ADJACENT",
                            "Slot " + s.id + " is in slot_row "
                                    + s.slotRow
                                    + " but others are in slot_row "
                                    + slotRow
                                    + ". Slots in different rows are "
                                    + "separated by a path lane and "
                                    + "cannot be combined.");
                }
            }

            // ── Guard 6: contiguous columns ───────────────────
            // Sort by col_start, then verify each consecutive
            // pair touches: col_start[i+1] == col_end[i] + 1
            slots.sort((a, b) -> Integer.compare(a.colStart, b.colStart));

            for (int i = 0; i < slots.size() - 1; i++) {
                Slot curr = slots.get(i);
                Slot next = slots.get(i + 1);
                if (next.colStart != curr.colEnd + 1) {
                    throw new ParkingException(
                            "NOT_ADJACENT",
                            "Slots are not contiguous. "
                                    + "Slot " + curr.id
                                    + " ends at col " + curr.colEnd
                                    + " but slot " + next.id
                                    + " starts at col " + next.colStart
                                    + ". There is a gap between them.");
                }
            }

            // ── Guard 7: newType must be registered ───────────
            int newSize = store.getVehicleSize(newType);
            if (newSize == -1) {
                throw new ParkingException(
                        "INVALID_TYPE",
                        "Vehicle type '" + newType
                                + "' is not registered in vehicle_size");
            }

            // ── Guard 8: sum of sizes must match ──────────────
            int totalSize = 0;
            int minSlotNum = Integer.MAX_VALUE;
            for (Slot s : slots) {
                totalSize += s.size;
                if (s.slotNum < minSlotNum) {
                    minSlotNum = s.slotNum;
                }
            }
            if (totalSize != newSize) {
                throw new ParkingException(
                        "INVALID_COMBINE",
                        "Sum of slot sizes (" + totalSize
                                + ") does not match target type '"
                                + newType + "' size (" + newSize + ")");
            }

            int colStart = slots.get(0).colStart;
            int colEnd = slots.get(slots.size() - 1).colEnd;

            // ── DB transaction: delete children, insert merged ─
            int newId = dao.combineSlots(
                    slotIds,
                    floor,
                    slotRow,
                    colStart,
                    colEnd,
                    minSlotNum,
                    newType,
                    newSize,
                    allowedMins,
                    rate,
                    penalty);

            // ── Update memory after commit ────────────────────
            // 1. Clear all child cells in gridMap
            for (Slot s : slots) {
                store.removeSlotFromGrid(
                        floor, slotRow, s.colStart, s.colEnd);
                store.removeSlotMeta(s.id);
            }

            // 2. Write merged slot into gridMap and slotMeta
            store.addSlotToGrid(
                    floor, slotRow, colStart, colEnd, newId);
            store.putSlotMeta(
                    newId, floor, slotRow,
                    colStart, colEnd, newSize);

            // 3. Register new type in occupiedSlots structure
            store.initFloorType(floor, newType);

            Map<String, Object> result = new HashMap<>();
            result.put("slot_id", newId);
            result.put("floor", floor);
            result.put("slot_row", slotRow);
            result.put("col_start", colStart);
            result.put("col_end", colEnd);
            result.put("type", newType);
            result.put("size", newSize);

            return result;

        } catch (ParkingException e) {
            throw e;
        } catch (SQLException e) {
            throw new ParkingException(
                    "DB_ERROR",
                    "Database error: " + e.getMessage(), e);
        }
    }

    // ── GET ALL VEHICLE SIZES ─────────────────────────────
    // Returns all vehicle_type → size mappings from memory.
    // No DB call — loaded at startup.
    public Map<String, Integer> getAllVehicleSizes() {
        return store.getAllVehicleSizes();
    }

}