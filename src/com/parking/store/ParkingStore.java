package com.parking.store;

import com.parking.model.ActiveVehicle;
import com.parking.model.Slot;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ParkingStore {
    // Returns the full grid for a given floor (for UI visualization)
    public int[][] getGrid(int floor) {
        int[][] grid = gridMap.get(floor);
        if (grid == null) {
            return new int[0][0];
        }
        return grid;
    }

    // ── SINGLETON ────────────────────────────────────────
    private static final ParkingStore INSTANCE = new ParkingStore();

    private ParkingStore() {
        this.occupiedSlots = new ConcurrentHashMap<>();
        this.activeVehicles = new ConcurrentHashMap<>();
        this.blockedFloors = new ConcurrentHashMap<>();
        this.occupiedSlotIds = ConcurrentHashMap.newKeySet();
        this.gridMap = new ConcurrentHashMap<>();
        this.slotMeta = new ConcurrentHashMap<>();
        this.vehicleSizes = new ConcurrentHashMap<>();
        this.floorReportCache = new ConcurrentHashMap<>();
    }

    public static ParkingStore getInstance() {
        return INSTANCE;
    }

    // ── EXISTING DATA STRUCTURES (unchanged) ─────────────

    // Floor → VehicleType → SlotId → Slot
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>>> occupiedSlots;

    // NumberPlate → ActiveVehicle
    private final ConcurrentHashMap<String, ActiveVehicle> activeVehicles;

    // FloorNum → Reason
    private final ConcurrentHashMap<Integer, String> blockedFloors;

    // All currently occupied slot_ids — kept in sync with
    // active_parking table. Used by split/combine to check
    // if a slot is free using only slot_id (no type needed).
    // O(1) contains check.
    private final java.util.concurrent.ConcurrentHashMap.KeySetView<Integer, Boolean> occupiedSlotIds;

    // ── NEW DATA STRUCTURES ───────────────────────────────

    // gridMap[floor][slotRow][col] → slot_id
    // 0 = free usable cell
    // -1 = dead cell (corridor column, never a slot)
    // >0 = slot_id occupying that cell
    //
    // Only slot rows are stored here — path rows are NOT stored.
    // This is the core space optimisation: your 12×8 floor has
    // 4 slot rows × 8 cols = 32 integers total, not 96.
    // Access is always O(1) — direct array index, no scan ever.
    //
    // floor → int[slotRows][cols]
    private final ConcurrentHashMap<Integer, int[][]> gridMap;

    // slotMeta[slot_id] → int[5]
    // [0] = floor
    // [1] = slotRow
    // [2] = colStart
    // [3] = colEnd
    // [4] = size
    //
    // Reverse lookup: given a slot_id, find its position in O(1).
    // Pure int array — no HashMap boxing, no GC pressure.
    // This is what makes combine validation O(1) per slot and
    // eliminates all grid scanning.
    //
    // slot_id → int[5]
    private final ConcurrentHashMap<Integer, int[]> slotMeta;

    // vehicleSizes: vehicle_type → size
    // Loaded once at startup from vehicle_size table.
    // Used by split/combine to validate size arithmetic.
    // O(1) lookup by type name.
    private final ConcurrentHashMap<String, Integer> vehicleSizes;

    // Floor report cache: dateRangeKey → { floorNum → List of stat rows }
    // Pre-loaded at startup, refreshed on every vehicle exit.
    // Eliminates all DB queries for floor report requests.
    private final ConcurrentHashMap<String, Map<Integer, List<Map<String, Object>>>> floorReportCache;

    // ── EXISTING METHODS (completely unchanged) ───────────

    public void addOccupied(int floor, String type,
            int slotId, Slot slot) {
        occupiedSlots
                .computeIfAbsent(floor, f -> new ConcurrentHashMap<>())
                .computeIfAbsent(type, t -> new ConcurrentHashMap<>())
                .put(slotId, slot);
        occupiedSlotIds.add(slotId);
    }

    public Slot removeOccupied(int floor, String type, int slotId) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>> floorMap = occupiedSlots.get(floor);
        if (floorMap == null)
            return null;
        ConcurrentHashMap<Integer, Slot> typeMap = floorMap.get(type);
        if (typeMap == null)
            return null;
        Slot removed = typeMap.remove(slotId);
        if (removed != null)
            occupiedSlotIds.remove(slotId);
        return removed;
    }

    public boolean isOccupied(int floor, String type, int slotId) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>> floorMap = occupiedSlots.get(floor);
        if (floorMap == null)
            return false;
        ConcurrentHashMap<Integer, Slot> typeMap = floorMap.get(type);
        if (typeMap == null)
            return false;
        return typeMap.containsKey(slotId);
    }

    public Set<Integer> getOccupiedIds(int floor, String type) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>> floorMap = occupiedSlots.get(floor);
        if (floorMap == null)
            return ConcurrentHashMap.newKeySet();
        ConcurrentHashMap<Integer, Slot> typeMap = floorMap.get(type);
        if (typeMap == null)
            return ConcurrentHashMap.newKeySet();
        return typeMap.keySet();
    }

    public int getOccupiedCount(int floor) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>> floorMap = occupiedSlots.get(floor);
        if (floorMap == null)
            return 0;
        return floorMap.values().stream()
                .mapToInt(Map::size).sum();
    }

    public int getOccupiedCountByType(int floor, String type) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>> floorMap = occupiedSlots.get(floor);
        if (floorMap == null)
            return 0;
        ConcurrentHashMap<Integer, Slot> typeMap = floorMap.get(type);
        if (typeMap == null)
            return 0;
        return typeMap.size();
    }

    public void updateOccupiedSlot(int floor, String type,
            int slotId, Slot newSlot) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>> floorMap = occupiedSlots.get(floor);
        if (floorMap == null)
            return;
        ConcurrentHashMap<Integer, Slot> typeMap = floorMap.get(type);
        if (typeMap == null)
            return;
        if (typeMap.containsKey(slotId))
            typeMap.put(slotId, newSlot);
    }

    public void initFloor(int floor) {
        occupiedSlots.computeIfAbsent(
                floor, f -> new ConcurrentHashMap<>());
    }

    public void initFloorType(int floor, String type) {
        occupiedSlots
                .computeIfAbsent(floor, f -> new ConcurrentHashMap<>())
                .computeIfAbsent(type, t -> new ConcurrentHashMap<>());
    }

    public boolean floorExists(int floor) {
        return occupiedSlots.containsKey(floor);
    }

    public Set<Integer> getAllFloors() {
        return occupiedSlots.keySet();
    }

    public Set<String> getTypesOnFloor(int floor) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>> floorMap = occupiedSlots.get(floor);
        if (floorMap == null)
            return ConcurrentHashMap.newKeySet();
        return floorMap.keySet();
    }

    public boolean addActiveVehicle(ActiveVehicle vehicle) {
        return activeVehicles.putIfAbsent(
                vehicle.numberPlate, vehicle) == null;
    }

    public ActiveVehicle getActiveVehicle(String plate) {
        return activeVehicles.get(plate);
    }

    public ActiveVehicle removeActiveVehicle(String plate) {
        return activeVehicles.remove(plate);
    }

    public Collection<ActiveVehicle> getAllActiveVehicles() {
        return activeVehicles.values();
    }

    public boolean isVehicleParked(String plate) {
        return activeVehicles.containsKey(plate);
    }

    public void blockFloor(int floor, String reason) {
        blockedFloors.put(floor, reason);
    }

    public void unblockFloor(int floor) {
        blockedFloors.remove(floor);
    }

    public boolean isFloorBlocked(int floor) {
        return blockedFloors.containsKey(floor);
    }

    public String getBlockReason(int floor) {
        return blockedFloors.get(floor);
    }

    public Set<Integer> getAllBlockedFloors() {
        return blockedFloors.keySet();
    }

    public ConcurrentHashMap<Integer, String> getAllBlockedFloorsWithReason() {
        return blockedFloors;
    }

    public Slot getOccupiedSlot(int floor, String type, int slotId) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Slot>> floorMap = occupiedSlots.get(floor);
        if (floorMap == null)
            return null;
        ConcurrentHashMap<Integer, Slot> typeMap = floorMap.get(type);
        if (typeMap == null)
            return null;
        return typeMap.get(slotId);
    }

    // O(1) check: is any vehicle currently parked in this slot?
    // Used by split/combine — we only have slot_id, not type.
    // occupiedSlotIds is kept in sync by addOccupied/removeOccupied.
    public boolean isVehicleParkedInSlot(int slotId) {
        return occupiedSlotIds.contains(slotId);
    }

    // ── NEW METHODS — gridMap ─────────────────────────────

    // Called once at startup per floor to allocate the grid
    // array. slotRows = number of slot rows on this floor.
    // cols = total_cols from parking_floors.
    // All cells initialised to 0 (free).
    // Dead columns (col 0, col > slot_col_end) are marked -1
    // by the caller (AppInitializer) after this call.
    public void initGrid(int floor, int slotRows, int cols) {
        gridMap.put(floor, new int[slotRows][cols]);
    }

    // Mark a single cell as dead (-1).
    // Called at startup for corridor columns (col 0, col 7…).
    public void markDead(int floor, int slotRow, int col) {
        int[][] grid = gridMap.get(floor);
        if (grid == null)
            return;
        grid[slotRow][col] = -1;
    }

    // Write slot_id into every column the slot occupies.
    // A bike (size=1) writes one cell.
    // A car (size=2) writes two consecutive cells.
    // A truck(size=4) writes four consecutive cells.
    // O(size) — always tiny (max 4 iterations).
    public void addSlotToGrid(int floor, int slotRow,
            int colStart, int colEnd, int slotId) {
        int[][] grid = gridMap.get(floor);
        if (grid == null)
            return;
        for (int c = colStart; c <= colEnd; c++) {
            grid[slotRow][c] = slotId;
        }
    }

    // Clear every column the slot occupied back to 0 (free).
    // Used during split and combine before writing new slots.
    public void removeSlotFromGrid(int floor, int slotRow,
            int colStart, int colEnd) {
        int[][] grid = gridMap.get(floor);
        if (grid == null)
            return;
        for (int c = colStart; c <= colEnd; c++) {
            grid[slotRow][c] = 0;
        }
    }

    // O(1) cell lookup — direct array index, never a scan.
    // Returns:
    // -1 → dead cell
    // 0 → free cell
    // >0 → slot_id occupying this cell
    public int getCell(int floor, int slotRow, int col) {
        int[][] grid = gridMap.get(floor);
        if (grid == null)
            return -1;
        if (slotRow < 0 || slotRow >= grid.length)
            return -1;
        if (col < 0 || col >= grid[0].length)
            return -1;
        return grid[slotRow][col];
    }

    // ── NEW METHODS — slotMeta ────────────────────────────

    // Store position metadata for a slot.
    // int[5]: [floor, slotRow, colStart, colEnd, size]
    // O(1) write.
    public void putSlotMeta(int slotId, int floor, int slotRow,
            int colStart, int colEnd, int size) {
        slotMeta.put(slotId,
                new int[] { floor, slotRow, colStart, colEnd, size });
    }

    // O(1) reverse lookup: given slot_id → position array.
    // Returns null if slot has no grid position assigned yet.
    public int[] getSlotMeta(int slotId) {
        return slotMeta.get(slotId);
    }

    // Remove a slot's metadata entry.
    // Called during split (parent removed) and combine
    // (all children removed).
    public void removeSlotMeta(int slotId) {
        slotMeta.remove(slotId);
    }

    // ── NEW METHODS — vehicleSizes ────────────────────────

    // Load all vehicle type → size mappings at startup.
    public void putVehicleSize(String vehicleType, int size) {
        vehicleSizes.put(vehicleType, size);
    }

    // O(1) size lookup by vehicle type name.
    // Returns -1 if type not found (should never happen after
    // FK constraint is in place).
    public int getVehicleSize(String vehicleType) {
        Integer size = vehicleSizes.get(vehicleType);
        return size != null ? size : -1;
    }

    // Returns a copy so callers cannot mutate the map.
    public java.util.Map<String, Integer> getAllVehicleSizes() {
        return new java.util.HashMap<>(vehicleSizes);
    }

    // ── FLOOR REPORT CACHE ────────────────────────────────

    // Store a fully-built floor report (all floors, per-type + ALL rows)
    // keyed by date range. Called after DB fetch or at startup.
    public void putFloorReport(String key,
            Map<Integer, List<Map<String, Object>>> data) {
        floorReportCache.put(key, data);
    }

    // O(1) cache lookup. Returns null on miss.
    public Map<Integer, List<Map<String, Object>>> getFloorReport(String key) {
        return floorReportCache.get(key);
    }

    // Wipe all cached reports so the next request re-fetches.
    // Called after every vehicle exit (history table changed).
    public void clearFloorReportCache() {
        floorReportCache.clear();
    }
}