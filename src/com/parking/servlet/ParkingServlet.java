package com.parking.servlet;

import com.parking.model.ActiveVehicle;
import com.parking.model.BillResult;
import com.parking.model.Slot;
import com.parking.service.ParkingException;
import com.parking.service.ParkingService;
import com.parking.util.JsonUtil;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@WebServlet("/parking/*")
public class ParkingServlet extends BaseServlet {

    private ParkingService service;

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public void init() throws ServletException {
        service = (ParkingService) getServletContext()
                .getAttribute("parkingService");
        if (service == null) {
            throw new ServletException(
                    "ParkingService not found in context");
        }
    }

    // ── POST ─────────────────────────────────────────────
    @Override
    protected void doPost(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        // check login
        if (!LoginServlet.isAuthorized(req)) {
            res.setStatus(401);
            res.getWriter().write(JsonUtil.error(
                    "UNAUTHORIZED", "Please login first"));
            return;
        }

        String path = req.getPathInfo();

        if ("/entry".equals(path)) {
            handleEntry(req, res);
        } else if ("/exit".equals(path)) {
            handleExit(req, res);
        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }
    }

    // ── GET ──────────────────────────────────────────────
    @Override
    protected void doGet(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        // check login
        if (!LoginServlet.isAuthorized(req)) {
            res.setStatus(401);
            res.getWriter().write(JsonUtil.error(
                    "UNAUTHORIZED", "Please login first"));
            return;
        }

        String path = req.getPathInfo();

        if (path == null) {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
            return;
        }

        if ("/availability".equals(path)) {
            handleAvailability(req, res);

        } else if ("/availability/all".equals(path)) {
            handleAvailabilityAll(req, res);

        } else if (path.startsWith("/history/")) {
            handleHistory(req, res, path);

        } else if (path.startsWith("/vehicle/")) {
            handleVehicleLookup(req, res, path);

        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }
    }

    // ── ENTRY ────────────────────────────────────────────
    private void handleEntry(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        String plate = req.getParameter("number_plate");
        String type = req.getParameter("vehicle_type");
        String floorP = req.getParameter("floor");
        String slotP = req.getParameter("slot_num");

        // parse optional params
        Integer floor = null;
        Integer slotNum = null;

        try {
            if (floorP != null && !floorP.isBlank()) {
                floor = Integer.parseInt(floorP.trim());
            }
            if (slotP != null && !slotP.isBlank()) {
                slotNum = Integer.parseInt(slotP.trim());
            }
        } catch (NumberFormatException e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS",
                    "floor and slot_num must be integers"));
            return;
        }

        try {
            Slot slot = service.vehicleEntry(
                    plate, floor, slotNum, type);

            String message = type + " parked at Floor "
                    + slot.floor + " Slot "
                    + slot.slotNum + " successfully";

            res.setStatus(200);
            res.getWriter().write(JsonUtil.entryResponse(
                    message,
                    plate,
                    type,
                    slot.floor,
                    slot.slotNum,
                    slot.id,
                    java.time.LocalDateTime.now().format(FMT)));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── EXIT ─────────────────────────────────────────────
    private void handleExit(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        String plate = req.getParameter("number_plate");

        try {
            BillResult bill = service.vehicleExit(plate);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.billResponse(bill));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── AVAILABILITY ─────────────────────────────────────
    private void handleAvailability(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        String floorP = req.getParameter("floor");
        String type = req.getParameter("vehicle_type");

        if (floorP == null || type == null) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS",
                    "floor and vehicle_type are required"));
            return;
        }

        try {
            int floor = Integer.parseInt(floorP.trim());
            Map<String, Object> result = service.getAvailability(floor, type);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.availabilityResponse(
                    (int) result.get("floor"),
                    (String) result.get("vehicle_type"),
                    (int) result.get("free_count"),
                    (java.util.Collection<Integer>) result.get("free_slots")));

        } catch (NumberFormatException e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS", "floor must be an integer"));
        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── AVAILABILITY ALL ─────────────────────────────────
    private void handleAvailabilityAll(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            List<Map<String, Object>> result = service.getAllAvailability();

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.allAvailabilityResponse(result));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── HISTORY ──────────────────────────────────────────
    private void handleHistory(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        // extract plate from /history/{plate}
        String plate = path.substring("/history/".length());

        if (plate.isBlank()) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS", "number_plate is required"));
            return;
        }

        try {
            List<BillResult> history = service.getParkingHistory(plate);

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.historyResponse(plate, history));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── VEHICLE LOOKUP ───────────────────────────────────
    private void handleVehicleLookup(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        // extract plate from /vehicle/{plate}
        String plate = path.substring("/vehicle/".length());

        if (plate.isBlank()) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS", "number_plate is required"));
            return;
        }

        try {
            ActiveVehicle vehicle = service.getVehicleDetails(plate);

            // get slotNum from DAO
            int slotNum = 0;
            try {
                com.parking.model.Slot slot = ((com.parking.dao.ParkingDAO) getServletContext()
                        .getAttribute("parkingDAO"))
                        .getSlotDetails(vehicle.slotId);

                if (slot != null)
                    slotNum = slot.slotNum;

            } catch (java.sql.SQLException ex) {
                System.err.println(
                        "[ParkingServlet] Could not get slot details: "
                                + ex.getMessage());
            }

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.vehicleResponse(vehicle, slotNum));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── RESOLVE HTTP STATUS FROM ERROR CODE ──────────────
    private int resolveStatus(String code) {
        switch (code) {
            case "UNAUTHORIZED":
                return 401;
            case "FLOOR_BLOCKED":
                return 403;
            case "VEHICLE_NOT_FOUND":
            case "SLOT_NOT_FOUND":
            case "FLOOR_NOT_FOUND":
                return 404;
            case "VEHICLE_ALREADY_PARKED":
            case "SLOT_OCCUPIED":
            case "SLOT_ALREADY_EXISTS":
            case "FLOOR_ALREADY_EXISTS":
            case "FLOOR_ALREADY_BLOCKED":
            case "FLOOR_NOT_BLOCKED":
                return 409;
            case "INVALID_PARAMS":
                return 400;
            case "DB_ERROR":
                return 500;
            default:
                return 500;
        }
    }
}
