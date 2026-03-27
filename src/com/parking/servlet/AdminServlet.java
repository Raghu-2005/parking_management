package com.parking.servlet;

import com.parking.dao.ParkingDAO;
import com.parking.model.BillResult;
import com.parking.service.ParkingException;
import com.parking.service.ParkingService;
import com.parking.util.JsonUtil;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@WebServlet("/admin/*")
public class AdminServlet extends BaseServlet {

    private ParkingService service;
    private ParkingDAO dao;

    @Override
    public void init() throws ServletException {
        service = (ParkingService) getServletContext()
                .getAttribute("parkingService");
        dao = (ParkingDAO) getServletContext()
                .getAttribute("parkingDAO");

        if (service == null || dao == null) {
            throw new ServletException(
                    "Service or DAO not found in context");
        }
    }

    // ── GET ──────────────────────────────────────────────
    @Override
    protected void doGet(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        if (!isAdmin(req, res))
            return;

        String path = req.getPathInfo();

        if (path == null) {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
            return;
        }

        if ("/active-vehicles".equals(path)) {
            handleGetActiveVehicles(req, res);

        } else if ("/history".equals(path)) {
            handleGetAllHistory(req, res);

        } else if ("/floors".equals(path)) {
            handleGetFloors(req, res);

        } else if (path.startsWith("/slots/")) {
            handleGetSlots(req, res, path);

        } else if ("/pending-users".equals(path)) {
            handleGetPendingUsers(req, res);

        } else if ("/users".equals(path)) {
            handleGetAllUsers(req, res);
        } else if ("/history/search".equals(path)) {
            handleSearchHistory(req, res);
        } else if ("/history/page".equals(path)) {
            handleSearchHistoryPage(req, res);
        } else if (path.matches("/floor-report/\\d+")) {
            handleGetFloorReport(req, res, path);
        } else if ("/vehicle-types".equals(path)) {
            handleGetVehicleTypes(req, res);
        } else if ("/allowed-mins".equals(path)) {
            handleGetAllowedMins(req, res);
        } else if ("/vehicle-size".equals(path)) {
            handleGetVehicleSize(req, res);
        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }
    }

    // ── POST ─────────────────────────────────────────────
    @Override
    protected void doPost(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        if (!isAdmin(req, res))
            return;

        String path = req.getPathInfo();

        if (path == null) {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
            return;
        }

        if ("/slots".equals(path)) {
            handleCreateSlot(req, res);

        } else if ("/floors".equals(path)) {
            handleCreateFloor(req, res);

        } else if (path.matches("/floors/\\d+/block")) {
            handleBlockFloor(req, res, path);

        } else if (path.matches("/users/\\d+/approve")) {
            handleApproveUser(req, res, path);

        } else if (path.matches("/users/\\d+/reject")) {
            handleRejectUser(req, res, path);

        } else if (path.matches("/slots/\\d+/split")) {
            handleSplitSlot(req, res, path);

        } else if ("/slots/combine".equals(path)) {
            handleCombineSlots(req, res);

        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }
    }

    // ── PUT ──────────────────────────────────────────────
    @Override
    protected void doPut(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        if (!isAdmin(req, res))
            return;

        String path = req.getPathInfo();

        if (path != null && path.matches("/slots/\\d+")) {
            handleUpdateSlot(req, res, path);
        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }
    }

    // ── DELETE ───────────────────────────────────────────
    @Override
    protected void doDelete(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        if (!isAdmin(req, res))
            return;

        String path = req.getPathInfo();

        if (path == null) {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
            return;
        }

        if (path.matches("/slots/\\d+")) {
            handleDeleteSlot(req, res, path);

        } else if (path.matches("/floors/\\d+/block")) {
            handleUnblockFloor(req, res, path);

        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }
    }

    // ── CREATE SLOT ──────────────────────────────────────
    private void handleCreateSlot(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            int floor = Integer.parseInt(
                    req.getParameter("floor"));
            int slotNum = Integer.parseInt(
                    req.getParameter("slot_num"));
            String type = req.getParameter("vehicle_type");
            int allowedMins = Integer.parseInt(
                    req.getParameter("allowed_mins"));
            double rate = Double.parseDouble(
                    req.getParameter("rate_per_hr"));
            double penalty = Double.parseDouble(
                    req.getParameter("penalty_per_hr"));

            int slotId = service.createSlot(
                    floor, slotNum, type,
                    allowedMins, rate, penalty);

            res.setStatus(200);
            res.getWriter().write(
                    "{"
                            + "\"status\":\"success\","
                            + "\"message\":\"Slot " + slotNum
                            + " created on Floor " + floor
                            + " for " + type + " successfully\","
                            + "\"slot_id\":" + slotId
                            + "}");

        } catch (NumberFormatException e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS",
                    "Invalid parameter format: " + e.getMessage()));
        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── UPDATE SLOT ──────────────────────────────────────
    private void handleUpdateSlot(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        try {
            int slotId = Integer.parseInt(
                    path.substring("/slots/".length()));

            // PUT body must be read manually — req.getParameter()
            // returns null for PUT in Jakarta Servlet
            StringBuilder sb = new StringBuilder();
            try (java.io.BufferedReader reader = req.getReader()) {
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
            }
            java.util.Map<String, String> params = new java.util.HashMap<>();
            for (String pair : sb.toString().split("&")) {
                String[] kv = pair.split("=");
                if (kv.length == 2) {
                    params.put(
                            java.net.URLDecoder.decode(kv[0], "UTF-8"),
                            java.net.URLDecoder.decode(kv[1], "UTF-8"));
                }
            }

            int allowedMins = Integer.parseInt(
                    params.get("allowed_mins"));
            double rate = Double.parseDouble(
                    params.get("rate_per_hr"));
            double penalty = Double.parseDouble(
                    params.get("penalty_per_hr"));

            service.updateSlot(slotId, allowedMins, rate, penalty);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.success(
                    "Slot updated successfully"));

        } catch (NumberFormatException e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS",
                    "Invalid parameter format: " + e.getMessage()));
        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── DELETE SLOT ──────────────────────────────────────
    private void handleDeleteSlot(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        try {
            int slotId = Integer.parseInt(
                    path.substring("/slots/".length()));

            service.deleteSlot(slotId);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.success(
                    "Slot deleted successfully"));

        } catch (NumberFormatException e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS", "slot_id must be an integer"));
        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── GET SLOTS BY FLOOR ───────────────────────────────
    private void handleGetSlots(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        try {
            int floor = Integer.parseInt(
                    path.substring("/slots/".length()));

            List<Map<String, Object>> slots = service.getSlotsByFloor(floor);

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.slotsResponse(floor, slots));

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

    // ── CREATE FLOOR ─────────────────────────────────────
    private void handleCreateFloor(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            int floor = Integer.parseInt(
                    req.getParameter("floor"));

            service.createFloor(floor);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.success(
                    "Floor " + floor
                            + " created successfully. "
                            + "Add slots using POST /admin/slots"));

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

    // ── GET FLOORS ───────────────────────────────────────
    private void handleGetFloors(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            List<Map<String, Object>> floors = service.getAllFloors();

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.floorsResponse(floors));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── BLOCK FLOOR ──────────────────────────────────────
    private void handleBlockFloor(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        try {
            String[] parts = path.split("/");
            int floor = Integer.parseInt(parts[2]);
            String reason = req.getParameter("reason");

            if (reason == null || reason.isBlank()) {
                res.setStatus(400);
                res.getWriter().write(JsonUtil.error(
                        "INVALID_PARAMS", "reason is required"));
                return;
            }

            service.blockFloor(floor, reason.trim());

            res.setStatus(200);
            res.getWriter().write(
                    "{"
                            + "\"status\":\"success\","
                            + "\"message\":\"Floor " + floor
                            + " blocked successfully\","
                            + "\"floor\":" + floor + ","
                            + "\"reason\":\"" + reason.trim() + "\""
                            + "}");

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

    // ── UNBLOCK FLOOR ────────────────────────────────────
    private void handleUnblockFloor(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        try {
            String[] parts = path.split("/");
            int floor = Integer.parseInt(parts[2]);

            service.unblockFloor(floor);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.success(
                    "Floor " + floor
                            + " unblocked successfully. Parking resumed."));

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

    // ── GET ACTIVE VEHICLES ──────────────────────────────
    // Optional params: floor (integer), type (car/bike/truck), page (1-based).
    // When floor and/or type are provided, filters the result set.
    // slot_num is joined inside the DAO — no N+1 DB calls.
    private void handleGetActiveVehicles(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            Integer floor = parseIntParam(req, "floor");
            String type = req.getParameter("type");
            if (type != null && type.trim().isEmpty())
                type = null;

            int page = 1;
            String pageParam = req.getParameter("page");
            if (pageParam != null && !pageParam.trim().isEmpty()) {
                try {
                    page = Math.max(1, Integer.parseInt(pageParam.trim()));
                } catch (NumberFormatException ignored) {
                }
            }
            final int PAGE_SIZE = 50;

            List<Map<String, Object>> vehicles = dao.getActiveVehiclesFiltered(floor, type, page, PAGE_SIZE);
            int totalCount = dao.getActiveVehiclesFilteredCount(floor, type);
            int totalPages = (int) Math.ceil((double) totalCount / PAGE_SIZE);

            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",");
            sb.append("\"metadata\":{");
            sb.append("\"total_count\":").append(totalCount).append(",");
            sb.append("\"total_pages\":").append(totalPages).append(",");
            sb.append("\"page_size\":").append(PAGE_SIZE).append(",");
            sb.append("\"current_page\":").append(page);
            if (floor != null)
                sb.append(",\"filter_floor\":").append(floor);
            if (type != null)
                sb.append(",\"filter_type\":\"").append(type).append("\"");
            sb.append("},");
            sb.append("\"vehicles\":[");
            for (int i = 0; i < vehicles.size(); i++) {
                Map<String, Object> v = vehicles.get(i);
                if (i > 0)
                    sb.append(",");
                sb.append("{");
                sb.append("\"number_plate\":\"").append(v.get("number_plate")).append("\",");
                sb.append("\"floor\":").append(v.get("floor_num")).append(",");
                sb.append("\"slot_num\":").append(v.get("slot_num")).append(",");
                sb.append("\"vehicle_type\":\"").append(v.get("vehicle_type")).append("\",");
                sb.append("\"entry_time\":\"").append(v.get("entry_time")).append("\"");
                sb.append("}");
            }
            sb.append("]}");

            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "DB_ERROR",
                    "Error fetching active vehicles: " + e.getMessage()));
        }
    }

    // ── GET ALL HISTORY ──────────────────────────────────
    // Paginated — page param (1-based), 50 rows per page.
    // Never loads all rows — safe on 10M+ table.
    private void handleGetAllHistory(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            int page = 1;
            String pageParam = req.getParameter("page");
            if (pageParam != null && !pageParam.trim().isEmpty()) {
                try {
                    page = Math.max(1, Integer.parseInt(pageParam.trim()));
                } catch (NumberFormatException ignored) {
                }
            }
            final int PAGE_SIZE = 50;

            List<BillResult> history = service.getAllHistory(page, PAGE_SIZE);

            res.setStatus(200);
            res.getWriter().write(
                    JsonUtil.historyResponse("ALL", history));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // ── GET PENDING USERS ─────────────────────────────────
    // Any active admin can see and approve all pending users,
    // including pending admin registrations.
    private void handleGetPendingUsers(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            List<String[]> pending = dao.getPendingUsers();

            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",\"pending\":[");
            boolean first = true;
            for (String[] u : pending) {
                if (!first)
                    sb.append(",");
                first = false;
                sb.append("{")
                        .append("\"id\":").append(u[0]).append(",")
                        .append("\"username\":\"").append(u[1]).append("\",")
                        .append("\"role\":\"").append(u[2]).append("\",")
                        .append("\"requested_at\":\"").append(u[3]).append("\"")
                        .append("}");
            }
            sb.append("]}");
            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error("DB_ERROR", e.getMessage()));
        }
    }

    // ── APPROVE USER ──────────────────────────────────────
    private void handleApproveUser(HttpServletRequest req,
            HttpServletResponse res,
            String path) throws IOException {
        try {
            int userId = Integer.parseInt(path.split("/")[2]);
            dao.approveUser(userId);
            res.setStatus(200);
            res.getWriter().write(JsonUtil.success(
                    "User approved successfully"));
        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "DB_ERROR", e.getMessage()));
        }
    }

    // ── REJECT USER ───────────────────────────────────────
    private void handleRejectUser(HttpServletRequest req,
            HttpServletResponse res,
            String path) throws IOException {
        try {
            int userId = Integer.parseInt(path.split("/")[2]);
            dao.rejectUser(userId);
            res.setStatus(200);
            res.getWriter().write(JsonUtil.success(
                    "User rejected successfully"));
        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "DB_ERROR", e.getMessage()));
        }
    }

    // ── ADMIN CHECK ──────────────────────────────────────
    private boolean isAdmin(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        if (!LoginServlet.isAuthorized(req)) {
            res.setStatus(401);
            res.getWriter().write(JsonUtil.error(
                    "UNAUTHORIZED", "Please login first"));
            return false;
        }

        if (!LoginServlet.isAdmin(req)) {
            res.setStatus(403);
            res.getWriter().write(JsonUtil.error(
                    "UNAUTHORIZED",
                    "Admin access required"));
            return false;
        }

        return true;
    }

    // ── RESOLVE HTTP STATUS ──────────────────────────────
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

    // ── GET ALL USERS (any active admin) ──────────────────
    private void handleGetAllUsers(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            List<String[]> users = dao.getAllUsers();

            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",\"users\":[");
            for (int i = 0; i < users.size(); i++) {
                String[] u = users.get(i);
                if (i > 0)
                    sb.append(",");
                sb.append("{")
                        .append("\"id\":").append(u[0]).append(",")
                        .append("\"username\":\"").append(u[1]).append("\",")
                        .append("\"role\":\"").append(u[2]).append("\",")
                        .append("\"status\":\"").append(u[3]).append("\",")
                        .append("\"joined\":\"").append(u[4]).append("\"")
                        .append("}");
            }
            sb.append("]}");
            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error("DB_ERROR", e.getMessage()));
        }
    }

    // ── SEARCH HISTORY ───────────────────────────────────
    // Single endpoint handles both initial search and page jumps.
    // page param defaults to 1 if not given.
    // All filters are optional — only given params are added to SQL.
    // Default 30-day window applied when no date given — ensures BRIN activates.
    //
    // Direct page jump example:
    // GET /admin/history/search?floor=1&type=car&page=7
    // Backend: OFFSET = (7-1) × 50 = 300 — no stored IDs needed
    //
    private void handleSearchHistory(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            // ── parse all filter params ───────────────────────────────
            Integer floor = parseIntParam(req, "floor");
            Integer slot = parseIntParam(req, "slot");
            String type = req.getParameter("type");
            String plate = req.getParameter("plate");
            LocalDateTime from = parseDateParam(req, "from");
            LocalDateTime to = parseDateParam(req, "to");
            String preset = req.getParameter("preset");
            Boolean penaltyOnly = "true".equals(req.getParameter("penalty_only"));
            Double minPenalty = parseDoubleParam(req, "min_penalty");
            Double maxPenalty = parseDoubleParam(req, "max_penalty");
            Double minAmount = parseDoubleParam(req, "min_amount");
            Double maxAmount = parseDoubleParam(req, "max_amount");
            Integer allowedMins = parseIntParam(req, "allowed_mins");
            Integer minTotalMins = parseIntParam(req, "min_total_mins");
            Integer maxTotalMins = parseIntParam(req, "max_total_mins");
            Integer minExtraMins = parseIntParam(req, "min_extra_mins");

            // ── page param — defaults to 1 ────────────────────────────
            int page = 1;
            String pageParam = req.getParameter("page");
            if (pageParam != null && !pageParam.trim().isEmpty()) {
                try {
                    page = Math.max(1, Integer.parseInt(pageParam.trim()));
                } catch (NumberFormatException ignored) {
                }
            }
            final int PAGE_SIZE = 50;

            // ── preset shortcuts ──────────────────────────────────────
            if (preset != null) {
                LocalDateTime now = LocalDateTime.now();
                switch (preset) {
                    case "today":
                        from = now.toLocalDate().atStartOfDay();
                        to = now;
                        break;
                    case "week":
                        from = now.minusDays(7);
                        to = now;
                        break;
                    case "month":
                        from = now.minusDays(30);
                        to = now;
                        break;
                }
            }

            // ── DEFAULT DATE WINDOW ───────────────────────────────────
            // No date given → apply last 30 days automatically.
            // This anchors the BRIN index on exit_date so it always activates.
            // Without this, floor-only or type-only filter = full seq scan.
            if (from == null && to == null) {
                to = LocalDateTime.now();
                from = to.minusDays(30);
            }

            if (type != null && type.trim().isEmpty())
                type = null;
            if (plate != null && plate.trim().isEmpty())
                plate = null;

            // ── STATS: total_count, total_revenue, total_penalty ──────
            // Cached 60 seconds — multiple admins with same filter hit cache
            Map<String, Object> stats = dao.getFilteredHistoryStats(
                    floor, slot, type, plate, from, to, penaltyOnly,
                    minPenalty, maxPenalty, minAmount, maxAmount,
                    allowedMins, minTotalMins, maxTotalMins, minExtraMins);

            int totalCount = ((Number) stats.get("total_count")).intValue();
            int totalPages = (int) Math.ceil((double) totalCount / PAGE_SIZE);

            // ── DATA: exactly PAGE_SIZE rows for requested page ───────
            // OFFSET = (page-1) × PAGE_SIZE
            // BRIN narrows table to date window first → OFFSET is fast
            List<Map<String, Object>> rows = dao.getFilteredHistoryPage(
                    floor, slot, type, plate, from, to, penaltyOnly,
                    minPenalty, maxPenalty, minAmount, maxAmount,
                    allowedMins, minTotalMins, maxTotalMins, minExtraMins,
                    page, PAGE_SIZE);

            // ── BUILD RESPONSE ────────────────────────────────────────
            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",");
            sb.append("\"metadata\":{");
            sb.append("\"total_count\":").append(totalCount).append(",");
            sb.append("\"total_pages\":").append(totalPages).append(",");
            sb.append("\"page_size\":").append(PAGE_SIZE).append(",");
            sb.append("\"current_page\":").append(page).append(",");
            sb.append("\"total_revenue\":").append(
                    String.format("%.2f", stats.get("total_revenue"))).append(",");
            sb.append("\"total_penalty\":").append(
                    String.format("%.2f", stats.get("total_penalty")));
            sb.append("},");
            sb.append("\"records\":[");
            appendHistoryRecords(sb, rows);
            sb.append("]}");

            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error("DB_ERROR",
                    "Filter query failed: " + e.getMessage()));
        }
    }

    // ── SEARCH HISTORY PAGE — direct page jump ────────────────────────
    // Accepts page number + all filter params — recalculates OFFSET server-side.
    // No ID list needed. No previous pages needed.
    // URL: GET /admin/history/page?floor=1&type=car&page=7
    private void handleSearchHistoryPage(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            Integer floor = parseIntParam(req, "floor");
            Integer slot = parseIntParam(req, "slot");
            String type = req.getParameter("type");
            String plate = req.getParameter("plate");
            LocalDateTime from = parseDateParam(req, "from");
            LocalDateTime to = parseDateParam(req, "to");
            Boolean penaltyOnly = "true".equals(req.getParameter("penalty_only"));
            Double minPenalty = parseDoubleParam(req, "min_penalty");
            Double maxPenalty = parseDoubleParam(req, "max_penalty");
            Double minAmount = parseDoubleParam(req, "min_amount");
            Double maxAmount = parseDoubleParam(req, "max_amount");
            Integer allowedMins = parseIntParam(req, "allowed_mins");
            Integer minTotalMins = parseIntParam(req, "min_total_mins");
            Integer maxTotalMins = parseIntParam(req, "max_total_mins");
            Integer minExtraMins = parseIntParam(req, "min_extra_mins");

            int page = 1;
            String pageParam = req.getParameter("page");
            if (pageParam != null && !pageParam.trim().isEmpty()) {
                try {
                    page = Math.max(1, Integer.parseInt(pageParam.trim()));
                } catch (NumberFormatException ignored) {
                }
            }

            if (from == null && to == null) {
                to = LocalDateTime.now();
                from = to.minusDays(30);
            }
            if (type != null && type.trim().isEmpty())
                type = null;
            if (plate != null && plate.trim().isEmpty())
                plate = null;

            List<Map<String, Object>> rows = dao.getFilteredHistoryPage(
                    floor, slot, type, plate, from, to, penaltyOnly,
                    minPenalty, maxPenalty, minAmount, maxAmount,
                    allowedMins, minTotalMins, maxTotalMins, minExtraMins,
                    page, 50);

            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",");
            sb.append("\"current_page\":").append(page).append(",");
            sb.append("\"records\":[");
            appendHistoryRecords(sb, rows);
            sb.append("]}");

            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error("DB_ERROR",
                    "Page query failed: " + e.getMessage()));
        }
    }

    // ── SHARED: append records array into StringBuilder ───────────────
    private void appendHistoryRecords(StringBuilder sb,
            List<Map<String, Object>> rows) {
        for (int i = 0; i < rows.size(); i++) {
            Map<String, Object> r = rows.get(i);
            if (i > 0)
                sb.append(",");
            sb.append("{");
            sb.append("\"id\":").append(r.get("id")).append(",");
            sb.append("\"number_plate\":\"").append(r.get("number_plate")).append("\",");
            sb.append("\"floor\":").append(r.get("floor")).append(",");
            sb.append("\"slot_num\":").append(
                    r.get("slot_num") == null ? "null" : r.get("slot_num")).append(",");
            sb.append("\"vehicle_type\":\"").append(r.get("vehicle_type")).append("\",");
            sb.append("\"entry_time\":\"").append(r.get("entry_time")).append("\",");
            sb.append("\"exit_time\":\"").append(r.get("exit_time")).append("\",");
            sb.append("\"allowed_mins\":").append(r.get("allowed_mins")).append(",");
            sb.append("\"total_mins\":").append(r.get("total_mins")).append(",");
            sb.append("\"extra_mins\":").append(r.get("extra_mins")).append(",");
            sb.append("\"total_amount\":").append(r.get("total_amount")).append(",");
            sb.append("\"penalty_amount\":").append(r.get("penalty_amount"));
            sb.append("}");
        }
    }

    // ── PARAM HELPERS ─────────────────────────────────────
    private Integer parseIntParam(HttpServletRequest req, String name) {
        String val = req.getParameter(name);
        if (val == null || val.trim().isEmpty())
            return null;
        try {
            return Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Long parseLongParam(HttpServletRequest req, String name) {
        String val = req.getParameter(name);
        if (val == null || val.trim().isEmpty())
            return null;
        try {
            return Long.parseLong(val.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Double parseDoubleParam(HttpServletRequest req, String name) {
        String val = req.getParameter(name);
        if (val == null || val.trim().isEmpty())
            return null;
        try {
            return Double.parseDouble(val.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private LocalDateTime parseDateParam(HttpServletRequest req, String name) {
        String val = req.getParameter(name);
        if (val == null || val.trim().isEmpty())
            return null;
        try {
            return LocalDateTime.parse(val.trim(),
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"));
        } catch (DateTimeParseException e) {
            try {
                return LocalDateTime.parse(val.trim() + "T00:00",
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"));
            } catch (DateTimeParseException e2) {
                return null;
            }
        }
    }

    // ── GET VEHICLE TYPES ─────────────────────────────────
    // GET /admin/vehicle-types
    // Returns all distinct vehicle types that have slots defined in the DB.
    // The frontend uses this to populate the vehicle type dropdown.
    // Any type the admin created slots for appears here automatically.
    // Response also includes a list of common suggested types so the frontend
    // can show a "custom" option for types not yet in the system.
    //
    // Example response:
    // {
    // "status":"success",
    // "types":["bike","bus","car","truck","van"],
    // "suggested":["auto","ambulance","bicycle","bus","car","jeep",
    // "minibus","motorcycle","suv","tractor","truck","van"]
    // }
    private void handleGetVehicleTypes(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            List<String> types = dao.getDistinctVehicleTypes();

            // common types shown in dropdown even before admin creates slots for them
            String[] suggested = {
                    "auto", "ambulance", "bicycle", "bus", "car",
                    "jeep", "minibus", "motorcycle", "suv",
                    "tractor", "truck", "van"
            };

            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",\"types\":[");
            for (int i = 0; i < types.size(); i++) {
                if (i > 0)
                    sb.append(",");
                sb.append("\"").append(types.get(i)).append("\"");
            }
            sb.append("],\"suggested\":[");
            for (int i = 0; i < suggested.length; i++) {
                if (i > 0)
                    sb.append(",");
                sb.append("\"").append(suggested[i]).append("\"");
            }
            sb.append("]}");

            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "DB_ERROR", "Failed to fetch vehicle types: " + e.getMessage()));
        }
    }

    // ── GET ALLOWED MINS ──────────────────────────────────
    // GET /admin/allowed-mins
    // Returns distinct allowed_mins values from parking_history.
    // Used to populate the allowed_mins dropdown in history filters.
    private void handleGetAllowedMins(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            List<Integer> mins = dao.getDistinctAllowedMins();
            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",\"allowed_mins\":[");
            for (int i = 0; i < mins.size(); i++) {
                if (i > 0)
                    sb.append(",");
                sb.append(mins.get(i));
            }
            sb.append("]}");
            res.setStatus(200);
            res.getWriter().write(sb.toString());
        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "DB_ERROR", "Failed to fetch allowed mins: " + e.getMessage()));
        }
    }

    // ── FLOOR REPORT ─────────────────────────────────────
    // Optional params: from (yyyy-MM-dd or yyyy-MM-ddTHH:mm), to (same)
    // Returns per-vehicle-type breakdown + ALL grand total row.
    //
    // Example response:
    // {
    // "status":"success",
    // "floor":1,
    // "period":{"from":"2026-01-01","to":"2026-03-17"},
    // "breakdown":[
    // {"vehicle_type":"car","total_vehicles":120,"total_revenue":24000.00,
    // "total_penalty":1200.00,"avg_duration_mins":45.5,"max_duration_mins":180},
    // {"vehicle_type":"bike",...},
    // {"vehicle_type":"ALL","total_vehicles":200,"total_revenue":35000.00,...}
    // ]
    // }
    private void handleGetFloorReport(HttpServletRequest req,
            HttpServletResponse res,
            String path) throws IOException {

        try {
            int floor = Integer.parseInt(
                    path.substring("/floor-report/".length()));

            LocalDateTime from = parseDateParam(req, "from");
            LocalDateTime to = parseDateParam(req, "to");

            // apply preset shortcuts identical to history search
            String preset = req.getParameter("preset");
            if (preset != null) {
                LocalDateTime now = LocalDateTime.now();
                switch (preset) {
                    case "today":
                        from = now.toLocalDate().atStartOfDay();
                        to = now;
                        break;
                    case "week":
                        from = now.minusDays(7);
                        to = now;
                        break;
                    case "month":
                        from = now.minusDays(30);
                        to = now;
                        break;
                }
            }

            // ── DEFAULT DATE WINDOW ───────────────────────────────────
            // floor_num alone on 10M+ rows = full sequential scan.
            // Defaulting to last 30 days anchors the BRIN index on exit_date
            // so only the relevant disk blocks are read.
            if (from == null && to == null) {
                to = LocalDateTime.now();
                from = to.minusDays(30);
            }

            List<Map<String, Object>> breakdown = dao.getFloorReport(floor, from, to);

            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",");
            sb.append("\"floor\":").append(floor).append(",");

            // period block — tells caller what date range was applied
            sb.append("\"period\":{");
            sb.append("\"from\":").append(from != null
                    ? "\"" + from.toLocalDate() + "\""
                    : "null").append(",");
            sb.append("\"to\":").append(to != null
                    ? "\"" + to.toLocalDate() + "\""
                    : "null");
            sb.append("},");

            sb.append("\"breakdown\":[");
            for (int i = 0; i < breakdown.size(); i++) {
                Map<String, Object> r = breakdown.get(i);
                if (i > 0)
                    sb.append(",");
                sb.append("{");
                sb.append("\"vehicle_type\":\"").append(r.get("vehicle_type")).append("\",");
                sb.append("\"total_vehicles\":").append(r.get("total_vehicles")).append(",");
                sb.append("\"total_revenue\":").append(r.get("total_revenue")).append(",");
                sb.append("\"total_penalty\":").append(r.get("total_penalty")).append(",");
                sb.append("\"avg_duration_mins\":").append(r.get("avg_duration_mins")).append(",");
                sb.append("\"max_duration_mins\":").append(r.get("max_duration_mins"));
                sb.append("}");
            }
            sb.append("]}");

            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (NumberFormatException e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS", "floor must be an integer"));
        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "DB_ERROR", "Floor report failed: " + e.getMessage()));
        }
    }

    // ── GET /admin/vehicle-size ───────────────────────────
    // Returns all vehicle types and their sizes.
    // Used by admin UI before split/combine to know what
    // target types are available and what sizes they map to.
    private void handleGetVehicleSize(HttpServletRequest req,
            HttpServletResponse res) throws IOException {

        try {
            Map<String, Integer> sizes = service.getAllVehicleSizes();

            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Map.Entry<String, Integer> e : sizes.entrySet()) {
                if (!first)
                    sb.append(",");
                sb.append("{\"vehicle_type\":\"")
                        .append(e.getKey())
                        .append("\",\"size\":")
                        .append(e.getValue())
                        .append("}");
                first = false;
            }
            sb.append("]");

            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "SERVER_ERROR", e.getMessage()));
        }
    }

    // ── POST /admin/slots/{id}/split ──────────────────────
    // Splits slot {id} into smaller slots of new_type.
    //
    // Request params:
    // new_type (required) — target vehicle type
    // allowed_mins (required) — billing config for children
    // rate_per_hr (required)
    // penalty_per_hr (required)
    //
    // Response: JSON array of created child slots
    private void handleSplitSlot(HttpServletRequest req,
            HttpServletResponse res,
            String path) throws IOException {

        int slotId;
        try {
            String[] parts = path.split("/");
            slotId = Integer.parseInt(parts[2]);
        } catch (Exception e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "BAD_REQUEST", "Invalid slot id in path"));
            return;
        }

        String newType = req.getParameter("new_type");
        String allowedMinsStr = req.getParameter("allowed_mins");
        String rateStr = req.getParameter("rate_per_hr");
        String penaltyStr = req.getParameter("penalty_per_hr");

        if (newType == null || newType.isBlank()
                || allowedMinsStr == null
                || rateStr == null
                || penaltyStr == null) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "BAD_REQUEST",
                    "new_type, allowed_mins, rate_per_hr, penalty_per_hr are all required"));
            return;
        }

        try {
            int allowedMins = Integer.parseInt(allowedMinsStr);
            double rate = Double.parseDouble(rateStr);
            double penalty = Double.parseDouble(penaltyStr);

            List<Map<String, Object>> children = service.splitSlot(
                    slotId,
                    newType.toLowerCase().trim(),
                    allowedMins,
                    rate,
                    penalty);

            StringBuilder sb = new StringBuilder(
                    "{\"status\":\"ok\","
                            + "\"message\":\"Slot split successfully\","
                            + "\"children\":[");

            for (int i = 0; i < children.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Map<String, Object> c = children.get(i);

                sb.append("{")
                        .append("\"slot_id\":").append(c.get("slot_id"))
                        .append(",\"floor\":").append(c.get("floor"))
                        .append(",\"slot_row\":").append(c.get("slot_row"))
                        .append(",\"col_start\":").append(c.get("col_start"))
                        .append(",\"col_end\":").append(c.get("col_end"))
                        .append(",\"type\":\"").append(c.get("type")).append("\"")
                        .append(",\"size\":").append(c.get("size"))
                        .append("}");
            }

            sb.append("]}");

            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (NumberFormatException e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "BAD_REQUEST",
                    "allowed_mins must be integer; rate_per_hr and penalty_per_hr must be numeric"));
        } catch (ParkingException e) {
            int status = "SLOT_NOT_FOUND".equals(e.getErrorCode())
                    ? 404
                    : "SLOT_OCCUPIED".equals(e.getErrorCode())
                            ? 409
                            : 400;

            res.setStatus(status);
            res.getWriter().write(
                    JsonUtil.error(e.getErrorCode(), e.getMessage()));
        }
    }

    // ── POST /admin/slots/combine ─────────────────────────
    // Combines multiple adjacent slots into one larger slot.
    //
    // Request params:
    // slot_ids (required) — comma-separated slot ids
    // e.g. "7,8" or "3,4,5,6"
    // new_type (required) — target vehicle type
    // allowed_mins (required)
    // rate_per_hr (required)
    // penalty_per_hr (required)
    //
    // Response: JSON object of the merged slot
    private void handleCombineSlots(HttpServletRequest req,
            HttpServletResponse res) throws IOException {

        String slotIdsParam = req.getParameter("slot_ids");
        String newType = req.getParameter("new_type");
        String allowedMinsStr = req.getParameter("allowed_mins");
        String rateStr = req.getParameter("rate_per_hr");
        String penaltyStr = req.getParameter("penalty_per_hr");

        if (slotIdsParam == null || slotIdsParam.isBlank()
                || newType == null || newType.isBlank()
                || allowedMinsStr == null
                || rateStr == null
                || penaltyStr == null) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "BAD_REQUEST",
                    "slot_ids, new_type, allowed_mins, rate_per_hr, penalty_per_hr are all required"));
            return;
        }

        try {
            List<Integer> slotIds = new ArrayList<>();
            for (String part : slotIdsParam.split(",")) {
                slotIds.add(Integer.parseInt(part.trim()));
            }

            int allowedMins = Integer.parseInt(allowedMinsStr);
            double rate = Double.parseDouble(rateStr);
            double penalty = Double.parseDouble(penaltyStr);

            Map<String, Object> merged = service.combineSlots(
                    slotIds,
                    newType.toLowerCase().trim(),
                    allowedMins,
                    rate,
                    penalty);

            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"ok\",")
                    .append("\"message\":\"Slots combined successfully\",")
                    .append("\"slot\":{")
                    .append("\"slot_id\":").append(merged.get("slot_id"))
                    .append(",\"floor\":").append(merged.get("floor"))
                    .append(",\"slot_row\":").append(merged.get("slot_row"))
                    .append(",\"col_start\":").append(merged.get("col_start"))
                    .append(",\"col_end\":").append(merged.get("col_end"))
                    .append(",\"type\":\"").append(merged.get("type")).append("\"")
                    .append(",\"size\":").append(merged.get("size"))
                    .append("}}");

            res.setStatus(200);
            res.getWriter().write(sb.toString());

        } catch (NumberFormatException e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "BAD_REQUEST",
                    "slot_ids must be integers; rate and penalty must be numeric"));
        } catch (ParkingException e) {
            int status = "SLOT_NOT_FOUND".equals(e.getErrorCode()) ? 404
                    : "SLOT_OCCUPIED".equals(e.getErrorCode()) ? 409
                            : "NOT_ADJACENT".equals(e.getErrorCode()) ? 400
                                    : "INVALID_COMBINE".equals(e.getErrorCode()) ? 400
                                            : "INVALID_SPLIT".equals(e.getErrorCode()) ? 400 : 400;

            res.setStatus(status);
            res.getWriter().write(
                    JsonUtil.error(e.getErrorCode(), e.getMessage()));
        }
    }

}