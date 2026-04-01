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
import java.time.LocalDate;
import java.time.LocalDateTime;
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
            throw new ServletException("ParkingService not found in context");
        }
    }

    // ── POST ─────────────────────────────────────────────
    @Override
    protected void doPost(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

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

        } else if ("/grid".equals(path)) {
            handleGrid(req, res);

        } else if ("/floors".equals(path)) {
            handleFloors(req, res);

        } else if ("/floor-report/all".equals(path)) {
            handleAllFloorReports(req, res);

        } else if (path.startsWith("/history/")) {
            handleHistory(req, res, path);

        } else if (path.startsWith("/vehicle/")) {
            handleVehicleLookup(req, res, path);

        } else if ("/occupied".equals(path)) {
            handleOccupied(req, res);
        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }

    }

    // Returns a JSON array of Slot objects for the given floor (manual JSON, no Gson/JsonUtil)
    private void handleGrid(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            int floor = Integer.parseInt(req.getParameter("floor"));
            com.parking.dao.ParkingDAO dao = (com.parking.dao.ParkingDAO) getServletContext().getAttribute("parkingDAO");
            List<com.parking.model.Slot> slots = dao.getSlotsByFloorObjects(floor);
            res.setContentType("application/json");
            StringBuilder json = new StringBuilder("[");
            for (int i = 0; i < slots.size(); i++) {
                com.parking.model.Slot s = slots.get(i);
                json.append("{")
                    .append("\"id\":").append(s.id).append(",")
                    .append("\"vehicleType\":\"").append(s.vehicleType).append("\",")
                    .append("\"size\":").append(s.size).append(",")
                    .append("\"slotRow\":").append(s.slotRow).append(",")
                    .append("\"colStart\":").append(s.colStart)
                    .append("}");
                if (i < slots.size() - 1) json.append(",");
            }
            json.append("]");
            res.getWriter().write(json.toString());
        } catch (Exception e) {
            res.setStatus(400);
            res.getWriter().write(com.parking.util.JsonUtil.error(
                    "INVALID_PARAMS", e.getMessage()));
        }
    }

    private void handleFloors(HttpServletRequest req,
            HttpServletResponse res) throws IOException {

        try {
            com.parking.dao.ParkingDAO dao =
                    (com.parking.dao.ParkingDAO) getServletContext()
                            .getAttribute("parkingDAO");

            List<Integer> floors = dao.getAllFloorNums();

            // FIX: convert List → JSON (no JsonUtil.toJson)
            res.setStatus(200);
            res.getWriter().write(floors.toString());

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(
                    com.parking.util.JsonUtil.error(
                            "SERVER_ERROR", e.getMessage()));
        }
    }

    // Returns a JSON array of occupied slot IDs for the given floor (manual JSON, no Gson/JsonUtil)
    private void handleOccupied(HttpServletRequest req, HttpServletResponse res) throws IOException {
        try {
            int floor = Integer.parseInt(req.getParameter("floor"));
            com.parking.dao.ParkingDAO dao = (com.parking.dao.ParkingDAO) getServletContext().getAttribute("parkingDAO");
            List<Integer> occupied = dao.getOccupiedSlots(floor);
            res.setContentType("application/json");
            res.getWriter().write(occupied.toString());
        } catch (Exception e) {
            res.setStatus(400);
            res.getWriter().write(com.parking.util.JsonUtil.error("INVALID_PARAMS", e.getMessage()));
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

            res.setStatus(200);
            res.getWriter().write(JsonUtil.entryResponse(
                    type + " parked successfully",
                    plate,
                    type,
                    slot.floor,
                    slot.slotNum,
                    slot.id,
                    LocalDateTime.now().format(FMT)));

        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
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
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── NEW FLOOR REPORT HANDLER ─────────────────────────
    private void handleAllFloorReports(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            String fromP = req.getParameter("from");
            String toP = req.getParameter("to");

            LocalDateTime from = null;
            LocalDateTime to = null;

            if (fromP != null && !fromP.isBlank()) {
                from = LocalDate.parse(fromP).atStartOfDay();
            }
            if (toP != null && !toP.isBlank()) {
                to = LocalDate.parse(toP).atStartOfDay();
            }

            Map<Integer, List<Map<String, Object>>> data = service.getAllFloorReports(from, to);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.floorReportAllResponse(data));

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "SERVER_ERROR", e.getMessage()));
        }
    }

    // ── AVAILABILITY ─────────────────────────────────────
    private void handleAvailability(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            int floor = Integer.parseInt(req.getParameter("floor"));
            String type = req.getParameter("vehicle_type");

            Map<String, Object> result = service.getAvailability(floor, type);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.availabilityResponse(
                    (int) result.get("floor"),
                    (String) result.get("vehicle_type"),
                    (int) result.get("free_count"),
                    (java.util.Collection<Integer>) result.get("free_slots")));

        } catch (Exception e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS", e.getMessage()));
        }
    }

    private void handleAvailabilityAll(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            List<Map<String, Object>> result = service.getAllAvailability();

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.allAvailabilityResponse(result));

        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── HISTORY ──────────────────────────────────────────
    private void handleHistory(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        String plate = path.substring("/history/".length());

        try {
            List<BillResult> history = service.getParkingHistory(plate);

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.historyResponse(plate, history));

        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── VEHICLE LOOKUP ───────────────────────────────────
    private void handleVehicleLookup(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        String plate = path.substring("/vehicle/".length());

        try {
            ActiveVehicle vehicle = service.getVehicleDetails(plate);

            int slotNum = 0;

            try {
                Slot slot = ((com.parking.dao.ParkingDAO) getServletContext().getAttribute("parkingDAO"))
                        .getSlotDetails(vehicle.slotId);

                if (slot != null)
                    slotNum = slot.slotNum;

            } catch (Exception ignored) {
            }

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.vehicleResponse(vehicle, slotNum));

        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── STATUS RESOLVER ──────────────────────────────────
    private int resolveStatus(String code) {
        switch (code) {
            case "UNAUTHORIZED":
                return 401;
            case "INVALID_PARAMS":
                return 400;
            case "FLOOR_BLOCKED":
                return 403;
            case "VEHICLE_NOT_FOUND":
            case "SLOT_NOT_FOUND":
                return 404;
            default:
                return 500;
        }
    }
}