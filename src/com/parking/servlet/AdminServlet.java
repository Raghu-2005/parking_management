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

    // Handles all GET requests under /admin/*
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

        } else if (path.startsWith("/layout/removed/")) {
            int floor = Integer.parseInt(path.substring("/layout/removed/".length()));
            handleGetRemovedSlots(req, res, floor);

        } else if (path.startsWith("/layout/history/")) {
            int floor = Integer.parseInt(path.substring("/layout/history/".length()));
            handleGetRemovalHistory(req, res, "/layout/history/" + floor);

        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }
    }

    // Handles all POST requests under /admin/*
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

        } else if ("/layout/remove/block".equals(path)) {
            handleRemoveBlock(req, res);

        } else if ("/layout/remove/slots".equals(path)) {
            handleRemoveSlots(req, res);

        } else if ("/layout/rollback/block".equals(path)) {
            handleRollbackBlock(req, res);

        } else if ("/layout/rollback/slots".equals(path)) {
            handleRollbackSlots(req, res);

        } else {
            res.setStatus(404);
            res.getWriter().write(JsonUtil.error(
                    "NOT_FOUND", "Endpoint not found"));
        }
    }

    // Handles all PUT requests under /admin/*
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

    // Handles all DELETE requests under /admin/*
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

    // POST /admin/slots — creates a new parking slot on the given floor
    private void handleCreateSlot(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            int floor = Integer.parseInt(req.getParameter("floor"));
            int slotNum = Integer.parseInt(req.getParameter("slot_num"));
            String type = req.getParameter("vehicle_type");
            int allowedMins = Integer.parseInt(req.getParameter("allowed_mins"));
            double rate = Double.parseDouble(req.getParameter("rate_per_hr"));
            double penalty = Double.parseDouble(req.getParameter("penalty_per_hr"));

            int slotId = service.createSlot(floor, slotNum, type, allowedMins, rate, penalty);

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

    // PUT /admin/slots/{id} — updates billing config (allowed_mins, rate, penalty)
    // for a slot
    // Note: req.getParameter() returns null for PUT bodies in Jakarta Servlet,
    // so the body is read manually and decoded from URL-encoded format.
    private void handleUpdateSlot(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        try {
            int slotId = Integer.parseInt(path.substring("/slots/".length()));

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

            int allowedMins = Integer.parseInt(params.get("allowed_mins"));
            double rate = Double.parseDouble(params.get("rate_per_hr"));
            double penalty = Double.parseDouble(params.get("penalty_per_hr"));

            service.updateSlot(slotId, allowedMins, rate, penalty);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.success("Slot updated successfully"));

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

    // DELETE /admin/slots/{id} — removes a slot permanently
    private void handleDeleteSlot(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        try {
            int slotId = Integer.parseInt(path.substring("/slots/".length()));

            service.deleteSlot(slotId);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.success("Slot deleted successfully"));

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

    // GET /admin/slots/{floor} — returns all slots on the given floor
    private int safeGetLayoutVersion(int floor) {
        try { return dao.getLayoutVersion(floor); } catch (Exception e) { return 1; }
    }
    private void handleGetSlots(HttpServletRequest req,
            HttpServletResponse res,
            String path)
            throws IOException {

        try {
            int floor = Integer.parseInt(path.substring("/slots/".length()));

            List<Map<String, Object>> slots = service.getSlotsByFloor(floor);

            res.setStatus(200);
            res.getWriter().write(JsonUtil.slotsResponse(floor, slots, safeGetLayoutVersion(floor)));

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

    // POST /admin/floors — creates a new floor with no slots
    // After creation, add slots via POST /admin/slots
    private void handleCreateFloor(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            int floor = Integer.parseInt(req.getParameter("floor"));

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

    // GET /admin/floors — returns all floors with their current status
    private void handleGetFloors(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        try {
            List<Map<String, Object>> floors = service.getAllFloors();

            res.setStatus(200);
            res.getWriter().write(JsonUtil.floorsResponse(floors));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // POST /admin/floors/{floor}/block — blocks a floor with a reason (no new
    // parking allowed)
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
                            + "\"message\":\"Floor " + floor + " blocked successfully\","
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

    // DELETE /admin/floors/{floor}/block — unblocks a floor and resumes normal
    // parking
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
                    "Floor " + floor + " unblocked successfully. Parking resumed."));

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

    // GET /admin/active-vehicles — returns currently parked vehicles
    // Optional filters: floor (int), type (car/bike/truck), page (1-based, default
    // 1)
    // Results are paginated at 50 per page
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

    // GET /admin/history — returns paginated parking history (50 records per page)
    // Use the page param (1-based) to navigate; safe for large tables
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
            res.getWriter().write(JsonUtil.historyResponse("ALL", history));

        } catch (ParkingException e) {
            int status = resolveStatus(e.getErrorCode());
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(
                    e.getErrorCode(), e.getMessage()));
        }
    }

    // GET /admin/pending-users — lists all users awaiting admin approval
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

    // POST /admin/users/{id}/approve — approves a pending user registration
    private void handleApproveUser(HttpServletRequest req,
            HttpServletResponse res,
            String path) throws IOException {
        try {
            int userId = Integer.parseInt(path.split("/")[2]);
            dao.approveUser(userId);
            res.setStatus(200);
            res.getWriter().write(JsonUtil.success("User approved successfully"));
        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error("DB_ERROR", e.getMessage()));
        }
    }

    // POST /admin/users/{id}/reject — rejects and removes a pending user
    // registration
    private void handleRejectUser(HttpServletRequest req,
            HttpServletResponse res,
            String path) throws IOException {
        try {
            int userId = Integer.parseInt(path.split("/")[2]);
            dao.rejectUser(userId);
            res.setStatus(200);
            res.getWriter().write(JsonUtil.success("User rejected successfully"));
        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error("DB_ERROR", e.getMessage()));
        }
    }

    // Checks that the request comes from a logged-in admin.
    // Returns false and writes the appropriate error response if the check fails.
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
                    "UNAUTHORIZED", "Admin access required"));
            return false;
        }

        return true;
    }

    // Maps a ParkingException error code to the appropriate HTTP status code
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

    // GET /admin/users — returns all registered users with their status and role
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

    // GET /admin/history/search — filtered, paginated history search
    // All filters are optional. Supports: floor, slot, type, plate,
    // from/to (yyyy-MM-dd or yyyy-MM-ddTHH:mm), preset (today/week/month),
    // penalty_only, min/max penalty, min/max amount, allowed_mins,
    // min/max total_mins, min_extra_mins, page (default 1).
    // Defaults to the last 30 days when no date range is provided.
    private void handleSearchHistory(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
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

            int page = 1;
            String pageParam = req.getParameter("page");
            if (pageParam != null && !pageParam.trim().isEmpty()) {
                try {
                    page = Math.max(1, Integer.parseInt(pageParam.trim()));
                } catch (NumberFormatException ignored) {
                }
            }
            final int PAGE_SIZE = 50;

            // Apply preset date shortcuts
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

            // Default to last 30 days when no date range is given.
            // This ensures the BRIN index on exit_date is always used,
            // preventing full sequential scans on large tables.
            if (from == null && to == null) {
                to = LocalDateTime.now();
                from = to.minusDays(30);
            }

            if (type != null && type.trim().isEmpty())
                type = null;
            if (plate != null && plate.trim().isEmpty())
                plate = null;

            // Fetch summary stats (total count, revenue, penalty) for the applied filters
            Map<String, Object> stats = dao.getFilteredHistoryStats(
                    floor, slot, type, plate, from, to, penaltyOnly,
                    minPenalty, maxPenalty, minAmount, maxAmount,
                    allowedMins, minTotalMins, maxTotalMins, minExtraMins);

            int totalCount = ((Number) stats.get("total_count")).intValue();
            int totalPages = (int) Math.ceil((double) totalCount / PAGE_SIZE);

            // Fetch one page of matching records
            List<Map<String, Object>> rows = dao.getFilteredHistoryPage(
                    floor, slot, type, plate, from, to, penaltyOnly,
                    minPenalty, maxPenalty, minAmount, maxAmount,
                    allowedMins, minTotalMins, maxTotalMins, minExtraMins,
                    page, PAGE_SIZE);

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

    // GET /admin/history/page — fetches a specific page for an existing search
    // Pass the same filter params along with the desired page number.
    // The offset is calculated server-side as (page-1) × 50.
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

    // Shared helper — appends a list of history records as JSON into the given
    // StringBuilder
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

    // Tries to parse a query parameter as Integer. Returns null if missing or
    // invalid.
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

    // Tries to parse a query parameter as Long. Returns null if missing or invalid.
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

    // Tries to parse a query parameter as Double. Returns null if missing or
    // invalid.
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

    // Parses a date query parameter in "yyyy-MM-ddTHH:mm" or "yyyy-MM-dd" format.
    // Returns null if the param is missing or cannot be parsed.
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

    // GET /admin/vehicle-types — returns all vehicle types that have slots defined,
    // plus a suggested list for populating the dropdown before any custom type is
    // added
    private void handleGetVehicleTypes(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            List<String> types = dao.getDistinctVehicleTypes();

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

    // GET /admin/allowed-mins — returns distinct allowed_mins values from history
    // Used to populate the allowed_mins filter dropdown in the history search UI
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

    // GET /admin/floor-report/{floor} — returns per-vehicle-type revenue breakdown
    // for a floor
    // Optional params: from, to (yyyy-MM-dd or yyyy-MM-ddTHH:mm), preset
    // (today/week/month)
    // Defaults to last 30 days when no date range is provided
    private void handleGetFloorReport(HttpServletRequest req,
            HttpServletResponse res,
            String path) throws IOException {

        try {
            int floor = Integer.parseInt(path.substring("/floor-report/".length()));

            LocalDateTime from = parseDateParam(req, "from");
            LocalDateTime to = parseDateParam(req, "to");

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

            // Default to last 30 days when no date range is given,
            // so the BRIN index on exit_date is used instead of a full table scan
            if (from == null && to == null) {
                to = LocalDateTime.now();
                from = to.minusDays(30);
            }

            List<Map<String, Object>> breakdown = dao.getFloorReport(floor, from, to);

            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",");
            sb.append("\"floor\":").append(floor).append(",");
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

    // GET /admin/vehicle-size — returns all vehicle types with their slot size
    // units
    // Used by the admin UI before split/combine to know what target types are
    // available
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

    // POST /admin/slots/{id}/split — splits a slot into smaller child slots of a
    // new vehicle type
    // Required params: new_type, allowed_mins, rate_per_hr, penalty_per_hr
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
            res.getWriter().write(JsonUtil.error(e.getErrorCode(), e.getMessage()));
        }
    }

    // POST /admin/slots/combine — merges adjacent slots into one larger slot
    // Required params:
    // slot_ids — comma-separated slot IDs, e.g. "7,8" or "3,4,5,6"
    // new_type — target vehicle type for the merged slot
    // allowed_mins, rate_per_hr, penalty_per_hr — billing config for the merged
    // slot
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
                            : 400;
            res.setStatus(status);
            res.getWriter().write(JsonUtil.error(e.getErrorCode(), e.getMessage()));
        }
    }
    // ── LAYOUT REMOVAL ───────────────────────────────────────────────────────

    // POST /admin/layout/remove/block
    private void handleRemoveBlock(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            int floor    = Integer.parseInt(req.getParameter("floor_num"));
            int rowStart = Integer.parseInt(req.getParameter("row_start"));
            int rowEnd   = Integer.parseInt(req.getParameter("row_end"));
            int colStart = Integer.parseInt(req.getParameter("col_start"));
            int colEnd   = Integer.parseInt(req.getParameter("col_end"));
            String reason = req.getParameter("reason");
            String removedBy = (String) req.getSession(false).getAttribute("username");

            Map<String, Object> result = service.removeBlock(
                floor, rowStart, rowEnd, colStart, colEnd, reason, removedBy);
            res.setStatus(result.get("success").equals(true) ? 200 : 409);
            res.getWriter().write(layoutResultJson(result));
        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(e.getErrorCode(), e.getMessage()));
        } catch (Exception e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error("INVALID_PARAMS",
                "Invalid request body: " + e.getMessage()));
        }
    }

    // POST /admin/layout/remove/slots
    private void handleRemoveSlots(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            int floor = Integer.parseInt(req.getParameter("floor_num"));
            String rawIds = req.getParameter("slot_ids");
            List<Integer> slotIds = new java.util.ArrayList<>();
            if (rawIds != null) {
                for (String s : rawIds.split(",")) {
                    String t = s.trim();
                    if (!t.isEmpty()) slotIds.add(Integer.parseInt(t));
                }
            }
            String reason = req.getParameter("reason");
            String removedBy = (String) req.getSession(false).getAttribute("username");

            Map<String, Object> result = service.removeSlots(
                floor, slotIds, reason, removedBy);
            res.setStatus(result.get("success").equals(true) ? 200 : 409);
            res.getWriter().write(layoutResultJson(result));
        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(e.getErrorCode(), e.getMessage()));
        } catch (Exception e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error("INVALID_PARAMS",
                "Invalid request body: " + e.getMessage()));
        }
    }

    // POST /admin/layout/rollback/block
    private void handleRollbackBlock(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            int floor    = Integer.parseInt(req.getParameter("floor_num"));
            int rowStart = Integer.parseInt(req.getParameter("row_start"));
            int rowEnd   = Integer.parseInt(req.getParameter("row_end"));
            int colStart = Integer.parseInt(req.getParameter("col_start"));
            int colEnd   = Integer.parseInt(req.getParameter("col_end"));
            String restoredBy = (String) req.getSession(false).getAttribute("username");
            Map<String, Object> result = service.rollbackBlock(
                floor, rowStart, rowEnd, colStart, colEnd, restoredBy);
            res.setStatus(200);
            res.getWriter().write(layoutResultJson(result));
        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(e.getErrorCode(), e.getMessage()));
        } catch (Exception e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error("INVALID_PARAMS",
                "Invalid request: " + e.getMessage()));
        }
    }

    // POST /admin/layout/rollback/slots
    private void handleRollbackSlots(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        try {
            int floor = Integer.parseInt(req.getParameter("floor_num"));
            String raw = req.getParameter("slot_ids");
            String restoredBy = (String) req.getSession(false).getAttribute("username");
            List<Integer> slotIds = new java.util.ArrayList<>();
            if (raw != null) {
                for (String s : raw.split(",")) {
                    String t = s.trim();
                    if (!t.isEmpty()) slotIds.add(Integer.parseInt(t));
                }
            }
            Map<String, Object> result = service.rollbackSlots(floor, slotIds, restoredBy);
            res.setStatus(200);
            res.getWriter().write(layoutResultJson(result));
        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(e.getErrorCode(), e.getMessage()));
        } catch (Exception e) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error("INVALID_PARAMS",
                "Invalid request: " + e.getMessage()));
        }
    }

    // GET /admin/layout/removed/{floor}
    // GET /admin/layout/removed/{floor}
    private void handleGetRemovedSlots(HttpServletRequest req,
            HttpServletResponse res, int floor) throws IOException {
        try {
            List<Map<String, Object>> slots = service.getRemovedSlotsByFloor(floor);
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append("\"slots\":[");
            for (int i = 0; i < slots.size(); i++) {
                Map<String, Object> s = slots.get(i);
                if (i > 0) sb.append(",");
                sb.append("{");
                sb.append("\"slot_id\":").append(s.get("slot_id")).append(",");
                sb.append("\"slot_num\":").append(s.get("slot_num")).append(",");
                sb.append("\"vehicle_type\":\"").append(s.get("vehicle_type")).append("\",");
                sb.append("\"slot_row\":").append(s.get("slot_row")).append(",");
                sb.append("\"col_start\":").append(s.get("col_start")).append(",");
                sb.append("\"col_end\":").append(s.get("col_end")).append(",");
                sb.append("\"size\":").append(s.get("size"));
                sb.append("}");
            }
            sb.append("]}");
            res.getWriter().write(sb.toString());
        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(e.getErrorCode(), e.getMessage()));
        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error("DB_ERROR", e.getMessage()));
        }
    }

    // GET /admin/layout/history/{floor}
    private void handleGetRemovalHistory(HttpServletRequest req,
            HttpServletResponse res, String path) throws IOException {
        try {
            int floor = Integer.parseInt(path.split("/")[3]);
            List<Map<String, Object>> history = service.getRemovalHistory(floor);
            StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"success\",\"floor\":").append(floor)
              .append(",\"history\":[");
            for (int i = 0; i < history.size(); i++) {
                Map<String, Object> r = history.get(i);
                sb.append("{");
                sb.append("\"id\":").append(r.get("id")).append(",");
                sb.append("\"floor_num\":").append(r.get("floor_num")).append(",");
                sb.append("\"row_start\":").append(r.get("row_start")).append(",");
                sb.append("\"row_end\":").append(r.get("row_end")).append(",");
                sb.append("\"col_start\":").append(r.get("col_start")).append(",");
                sb.append("\"col_end\":").append(r.get("col_end")).append(",");
                @SuppressWarnings("unchecked")
                java.util.List<Integer> ids = r.get("slot_ids") != null ? (java.util.List<Integer>) r.get("slot_ids") : new java.util.ArrayList<>();
                sb.append("\"slot_ids\":").append(ids.toString()).append(",").append("\"slot_count\":").append(ids.size()).append(",");
                sb.append("\"reason\":").append(lqQuote((String) r.get("reason"))).append(",");
                sb.append("\"removed_by\":").append(lqQuote((String) r.get("removed_by"))).append(",");
                sb.append("\"removed_at\":").append(lqQuote((String) r.get("removed_at"))).append(",");
                sb.append("\"layout_version\":").append(r.get("layout_version_at_removal"));
                sb.append("}");
                if (i < history.size() - 1) sb.append(",");
            }
            sb.append("]}");
            res.setStatus(200);
            res.getWriter().write(sb.toString());
        } catch (ParkingException e) {
            res.setStatus(resolveStatus(e.getErrorCode()));
            res.getWriter().write(JsonUtil.error(e.getErrorCode(), e.getMessage()));
        }
    }

    private String layoutResultJson(Map<String, Object> r) {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"success\":").append(r.get("success")).append(",");
        if (r.get("conflict") == null) {
            sb.append("\"conflict\":null,");
        } else {
            sb.append("\"conflict\":").append(lqQuote(r.get("conflict").toString())).append(",");
        }
        if (r.containsKey("slots_removed")) {
            sb.append("\"slots_removed\":").append(r.get("slots_removed")).append(",");
            sb.append("\"layout_version\":").append(r.get("layout_version")).append(",");
            sb.append("\"removed_region_id\":").append(r.get("removed_region_id"));
        } else {
            @SuppressWarnings("unchecked")
            List<?> details = (List<?>) r.get("details");
            sb.append("\"details\":[");
            for (int i = 0; i < details.size(); i++) {
                @SuppressWarnings("unchecked")
                Map<String, Object> d = (Map<String, Object>) details.get(i);
                sb.append("{");
                if (d.containsKey("slot_id")) {
                    sb.append("\"slot_id\":").append(d.get("slot_id"));
                    sb.append(",\"number_plate\":").append(lqQuote((String) d.get("number_plate")));
                } else {
                    sb.append("\"slot_id\":").append(d.get("slot_id"));
                }
                sb.append("}");
                if (i < details.size() - 1) sb.append(",");
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    private String lqQuote(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }

}
