package com.parking.servlet;

import com.parking.dao.ParkingDAO;
import com.parking.model.ActiveVehicle;
import com.parking.model.Slot;
import com.parking.pool.ConnectionPool;
import com.parking.service.ParkingService;
import com.parking.store.ParkingStore;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@WebListener
public class AppInitializer implements ServletContextListener {

    private ConnectionPool pool;

    @Override
    public void contextInitialized(ServletContextEvent sce) {

        ServletContext ctx = sce.getServletContext();

        System.out.println("[App] Starting Parking System...");

        try {
            // 🔥 FIX 1 — LOAD POSTGRES DRIVER (CRITICAL)
            try {
                Class.forName("org.postgresql.Driver");
                System.out.println("[App] PostgreSQL driver loaded");
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("PostgreSQL Driver NOT FOUND!", e);
            }

            // ── Step 1: read DB config
            String dbUrl = ctx.getInitParameter("db.url");
            String dbUser = ctx.getInitParameter("db.username");
            String dbPassword = ctx.getInitParameter("db.password");
            int poolSize = Integer.parseInt(ctx.getInitParameter("db.poolSize"));

            // ── Step 2: create connection pool
            pool = new ConnectionPool(dbUrl, dbUser, dbPassword, poolSize);
            System.out.println("[App] Connection pool created");

            // ── Step 3: DAO
            ParkingDAO dao = new ParkingDAO(pool);
            System.out.println("[App] DAO created");

            // ── Step 4: Store
            ParkingStore store = ParkingStore.getInstance();
            System.out.println("[App] Store initialized");

            // ── Step 5: load slots
            List<Slot> allSlots = dao.loadAllSlots();
            for (Slot slot : allSlots) {
                store.initFloorType(slot.floor, slot.vehicleType);
            }
            System.out.println("[App] Loaded " + allSlots.size() + " slots into store");

            // ── Step 6: restore active vehicles
            List<ActiveVehicle> activeVehicles = dao.loadActiveVehicles();

            for (ActiveVehicle vehicle : activeVehicles) {
                store.addActiveVehicle(vehicle);

                for (Slot slot : allSlots) {
                    if (slot.id == vehicle.slotId) {
                        store.addOccupied(
                                vehicle.floor,
                                vehicle.vehicleType,
                                vehicle.slotId,
                                slot);
                        break;
                    }
                }
            }
            System.out.println("[App] Restored " + activeVehicles.size() + " active vehicles");

            // ── Step 7: blocked floors
            Map<Integer, String> blocked = dao.loadBlockedFloors();
            for (Map.Entry<Integer, String> entry : blocked.entrySet()) {
                store.blockFloor(entry.getKey(), entry.getValue());
            }
            System.out.println("[App] Restored " + blocked.size() + " blocked floors");

            // ── Step 8: service
            ParkingService service = new ParkingService(dao, store);
            System.out.println("[App] Service created");

            ctx.setAttribute("parkingService", service);
            ctx.setAttribute("parkingDAO", dao);

            // ── Step 9: vehicle sizes
            Map<String, Integer> vehicleSizes = dao.loadVehicleSizes();
            for (Map.Entry<String, Integer> entry : vehicleSizes.entrySet()) {
                store.putVehicleSize(entry.getKey(), entry.getValue());
            }
            System.out.println("[App] Loaded " + vehicleSizes.size() + " vehicle sizes");

            // ── Step 10: grid build
            List<Map<String, Integer>> floorConfigs = dao.loadParkingFloors();

            for (Map<String, Integer> fc : floorConfigs) {
                int floorNum = fc.get("floor_num");
                int totalCols = fc.get("total_cols");
                int pathWidth = fc.get("path_width");
                int slotColStart = fc.get("slot_col_start");
                int slotColEnd = fc.get("slot_col_end");
                int totalRows = fc.get("total_rows");

                int slotRowCount = totalRows / (1 + pathWidth);

                store.initGrid(floorNum, slotRowCount, totalCols);

                for (int sr = 0; sr < slotRowCount; sr++) {
                    for (int c = 0; c < slotColStart; c++) {
                        store.markDead(floorNum, sr, c);
                    }
                    for (int c = slotColEnd + 1; c < totalCols; c++) {
                        store.markDead(floorNum, sr, c);
                    }
                }
            }

            int gridSlotCount = 0;
            for (Slot slot : allSlots) {
                if (!slot.hasGridPosition())
                    continue;

                store.addSlotToGrid(
                        slot.floor,
                        slot.slotRow,
                        slot.colStart,
                        slot.colEnd,
                        slot.id);

                store.putSlotMeta(
                        slot.id,
                        slot.floor,
                        slot.slotRow,
                        slot.colStart,
                        slot.colEnd,
                        slot.size);

                gridSlotCount++;
            }

            System.out.println("[App] Built grid for " + floorConfigs.size()
                    + " floor(s), " + gridSlotCount + " slots");

            // ── Step 11: pre-load floor report cache
            /*
             * try {
             * service.getAllFloorReports(null, null);
             * System.out.println("[App] Floor report cache pre-loaded");
             * } catch (Exception e) {
             * System.err.println("[App] WARN — floor report cache pre-load failed: "
             * + e.getMessage());
             * }
             */

            System.out.println("[App] Parking System started successfully");

        } catch (SQLException e) {
            System.err.println("[App] FATAL — failed to start: " + e.getMessage());
            throw new RuntimeException("Startup failed", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        System.out.println("[App] Shutting down Parking System...");
        if (pool != null)
            pool.shutdown();
        System.out.println("[App] Shutdown complete");
    }
}