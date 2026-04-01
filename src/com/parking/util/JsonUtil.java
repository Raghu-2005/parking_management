package com.parking.util;

import com.parking.model.ActiveVehicle;
import com.parking.model.BillResult;
import com.parking.model.Slot;

import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class JsonUtil {
    // Generic toJson for int[][] and other objects (simple, not for production)
    public static String toJson(int[][] grid) {
        if (grid == null) return "null";
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < grid.length; i++) {
            if (i > 0) sb.append(",");
            sb.append("[");
            for (int j = 0; j < grid[i].length; j++) {
                if (j > 0) sb.append(",");
                sb.append(grid[i][j]);
            }
            sb.append("]");
        }
        sb.append("]");
        return sb.toString();
    }

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // ── BASIC RESPONSES ──────────────────────────────────

    public static String success(String message) {
        return "{"
                + "\"status\":\"success\","
                + "\"message\":" + quote(message)
                + "}";
    }

    public static String error(String code, String message) {
        return "{"
                + "\"status\":\"error\","
                + "\"code\":" + quote(code) + ","
                + "\"message\":" + quote(message)
                + "}";
    }

    // ── DETECT ERROR TYPE FROM EXCEPTION ─────────────────

    public static int resolveStatus(Exception e) {
        if (e == null)
            return 500;
        String msg = e.getMessage();
        if (msg == null)
            return 500;
        if (msg.contains("No connection available"))
            return 503;
        if (msg.contains("SLOT_OCCUPIED"))
            return 409;
        if (msg.contains("ALREADY_PARKED"))
            return 409;
        if (msg.contains("FLOOR_BLOCKED"))
            return 403;
        if (msg.contains("NOT_FOUND"))
            return 404;
        if (msg.contains("BAD_REQUEST"))
            return 400;
        return 500;
    }

    public static String resolveCode(Exception e) {
        if (e == null)
            return "SERVER_ERROR";
        String msg = e.getMessage();
        if (msg == null)
            return "SERVER_ERROR";
        if (msg.contains("No connection available"))
            return "POOL_TIMEOUT";
        if (msg.contains("SLOT_OCCUPIED"))
            return "SLOT_OCCUPIED";
        if (msg.contains("ALREADY_PARKED"))
            return "ALREADY_PARKED";
        if (msg.contains("FLOOR_BLOCKED"))
            return "FLOOR_BLOCKED";
        if (msg.contains("NOT_FOUND"))
            return "NOT_FOUND";
        if (msg.contains("BAD_REQUEST"))
            return "BAD_REQUEST";
        return "DB_ERROR";
    }

    // ── ENTRY RESPONSE ───────────────────────────────────

    public static String entryResponse(String message,
            String plate, String type,
            int floor, int slotNum,
            int slotId, String entryTime) {
        return "{"
                + "\"status\":" + quote("success") + ","
                + "\"message\":" + quote(message) + ","
                + "\"number_plate\":" + quote(plate) + ","
                + "\"vehicle_type\":" + quote(type) + ","
                + "\"floor\":" + floor + ","
                + "\"slot_num\":" + slotNum + ","
                + "\"slot_id\":" + slotId + ","
                + "\"entry_time\":" + quote(entryTime)
                + "}";
    }

    // ── BILL RESPONSE ────────────────────────────────────

    public static String billResponse(BillResult bill) {
        return "{"
                + "\"status\":" + quote("success") + ","
                + "\"message\":" + quote("Vehicle exited successfully") + ","
                + "\"number_plate\":" + quote(bill.numberPlate) + ","
                + "\"vehicle_type\":" + quote(bill.vehicleType) + ","
                + "\"floor\":" + bill.floor + ","
                + "\"slot_num\":" + bill.slotNum + ","
                + "\"entry_time\":" + quote(bill.entryTime.format(FMT)) + ","
                + "\"exit_time\":" + quote(bill.exitTime.format(FMT)) + ","
                + "\"allowed_mins\":" + bill.allowedMins + ","
                + "\"total_mins_parked\":" + bill.totalMins + ","
                + "\"extra_mins\":" + bill.extraMins + ","
                + "\"total_amount\":" + bill.totalAmount + ","
                + "\"penalty_amount\":" + bill.penaltyAmount
                + "}";
    }

    // ── VEHICLE RESPONSE ─────────────────────────────────

    public static String vehicleResponse(ActiveVehicle v, int slotNum) {
        return "{"
                + "\"status\":" + quote("success") + ","
                + "\"number_plate\":" + quote(v.numberPlate) + ","
                + "\"vehicle_type\":" + quote(v.vehicleType) + ","
                + "\"floor\":" + v.floor + ","
                + "\"slot_num\":" + slotNum + ","
                + "\"slot_id\":" + v.slotId + ","
                + "\"entry_time\":" + quote(v.entryTime.format(FMT))
                + "}";
    }

    // ── AVAILABILITY RESPONSE ────────────────────────────

    public static String availabilityResponse(int floor, String type,
            int freeCount, Collection<Integer> freeSlots) {
        StringBuilder slots = new StringBuilder("[");
        boolean first = true;
        for (int s : freeSlots) {
            if (!first)
                slots.append(",");
            slots.append(s);
            first = false;
        }
        slots.append("]");
        return "{"
                + "\"status\":" + quote("success") + ","
                + "\"floor\":" + floor + ","
                + "\"vehicle_type\":" + quote(type) + ","
                + "\"free_count\":" + freeCount + ","
                + "\"free_slots\":" + slots
                + "}";
    }

    // ── ALL AVAILABILITY RESPONSE ────────────────────────

    public static String allAvailabilityResponse(
            List<Map<String, Object>> data) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"status\":\"success\",\"availability\":[");
        for (int i = 0; i < data.size(); i++) {
            Map<String, Object> row = data.get(i);
            sb.append("{")
                    .append("\"floor\":").append(row.get("floor")).append(",")
                    .append("\"vehicle_type\":").append(
                            quote((String) row.get("vehicle_type")))
                    .append(",")
                    .append("\"free_count\":").append(row.get("free_count")).append(",")
                    .append("\"total\":").append(row.get("total"))
                    .append("}");
            if (i < data.size() - 1)
                sb.append(",");
        }
        sb.append("]}");
        return sb.toString();
    }

    // ── ACTIVE VEHICLES RESPONSE ─────────────────────────

    public static String activeVehiclesResponse(
            Collection<ActiveVehicle> vehicles,
            Map<Integer, Integer> slotIdToSlotNum) {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("\"status\":\"success\",")
                .append("\"count\":").append(vehicles.size()).append(",")
                .append("\"vehicles\":[");
        boolean first = true;
        for (ActiveVehicle v : vehicles) {
            if (!first)
                sb.append(",");
            int slotNum = slotIdToSlotNum.getOrDefault(v.slotId, 0);
            sb.append("{")
                    .append("\"number_plate\":").append(quote(v.numberPlate)).append(",")
                    .append("\"vehicle_type\":").append(quote(v.vehicleType)).append(",")
                    .append("\"floor\":").append(v.floor).append(",")
                    .append("\"slot_num\":").append(slotNum).append(",")
                    .append("\"entry_time\":").append(quote(v.entryTime.format(FMT)))
                    .append("}");
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }

    // ── HISTORY RESPONSE ─────────────────────────────────

    public static String historyResponse(String plate,
            List<BillResult> history) {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("\"status\":\"success\",")
                .append("\"number_plate\":").append(quote(plate)).append(",")
                .append("\"total_records\":").append(history.size()).append(",")
                .append("\"history\":[");
        boolean first = true;
        for (BillResult b : history) {
            if (!first)
                sb.append(",");
            sb.append("{")
                    .append("\"number_plate\":").append(quote(b.numberPlate)).append(",")
                    .append("\"floor\":").append(b.floor).append(",")
                    .append("\"slot_num\":").append(b.slotNum).append(",")
                    .append("\"vehicle_type\":").append(quote(b.vehicleType)).append(",")
                    .append("\"entry_time\":").append(quote(b.entryTime.format(FMT))).append(",")
                    .append("\"exit_time\":").append(quote(b.exitTime.format(FMT))).append(",")
                    .append("\"allowed_mins\":").append(b.allowedMins).append(",")
                    .append("\"total_mins\":").append(b.totalMins).append(",")
                    .append("\"extra_mins\":").append(b.extraMins).append(",")
                    .append("\"total_amount\":").append(b.totalAmount).append(",")
                    .append("\"penalty_amount\":").append(b.penaltyAmount)
                    .append("}");
            first = false;
        }
        sb.append("]}");
        return sb.toString();
    }

    // ── FLOORS RESPONSE ──────────────────────────────────

    public static String floorsResponse(List<Map<String, Object>> floors) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"status\":\"success\",\"floors\":[");
        for (int i = 0; i < floors.size(); i++) {
            Map<String, Object> f = floors.get(i);
            sb.append("{")
                    .append("\"floor\":").append(f.get("floor")).append(",")
                    .append("\"status\":").append(quote((String) f.get("status"))).append(",")
                    .append("\"total_slots\":").append(f.get("total_slots")).append(",")
                    .append("\"occupied\":").append(f.get("occupied")).append(",")
                    .append("\"free\":").append(f.get("free"));
            if (f.containsKey("reason")) {
                sb.append(",\"reason\":").append(quote((String) f.get("reason")));
            }
            sb.append("}");
            if (i < floors.size() - 1)
                sb.append(",");
        }
        sb.append("]}");
        return sb.toString();
    }

    // ── SLOTS RESPONSE ───────────────────────────────────

    public static String slotsResponse(int floor,
            List<Map<String, Object>> slots, int layoutVersion) {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
                .append("\"status\":\"success\",")
                .append("\"floor\":").append(floor).append(",")
                .append("\"layout_version\":").append(layoutVersion).append(",")
                .append("\"slots\":[");
        for (int i = 0; i < slots.size(); i++) {
            Map<String, Object> s = slots.get(i);
            sb.append("{")
                    .append("\"slot_id\":").append(s.get("slot_id")).append(",")
                    .append("\"slot_num\":").append(s.get("slot_num")).append(",")
                    .append("\"vehicle_type\":").append(
                            quote((String) s.get("vehicle_type")))
                    .append(",")
                    .append("\"allowed_mins\":").append(s.get("allowed_mins")).append(",")
                    .append("\"rate_per_hr\":").append(s.get("rate_per_hr")).append(",")
                    .append("\"penalty_per_hr\":").append(s.get("penalty_per_hr")).append(",")
                    .append("\"status\":").append(quote((String) s.get("status")));
            if (s.get("number_plate") != null) {
                sb.append(",\"number_plate\":").append(
                        quote((String) s.get("number_plate")));
            } else {
                sb.append(",\"number_plate\":null");
            }
            sb.append(",\"slot_row\":").append(s.get("slot_row"));
            sb.append(",\"col_start\":").append(s.get("col_start"));
            sb.append(",\"col_end\":").append(s.get("col_end"));
            sb.append(",\"size\":").append(s.get("size"));
            sb.append("}");
            if (i < slots.size() - 1)
                sb.append(",");
        }
        sb.append("]}");
        return sb.toString();
    }

    public static String floorReportAllResponse(
            Map<Integer, List<Map<String, Object>>> data) {

        StringBuilder sb = new StringBuilder();
        sb.append("{\"status\":\"success\",\"data\":{");

        boolean firstFloor = true;

        for (Map.Entry<Integer, List<Map<String, Object>>> entry : data.entrySet()) {

            if (!firstFloor)
                sb.append(",");
            firstFloor = false;

            int floor = entry.getKey();
            List<Map<String, Object>> rows = entry.getValue();

            sb.append("\"").append(floor).append("\":[");

            boolean firstRow = true;

            for (Map<String, Object> r : rows) {

                if (!firstRow)
                    sb.append(",");
                firstRow = false;

                sb.append("{")
                        .append("\"vehicle_type\":").append(quote((String) r.get("vehicle_type"))).append(",")
                        .append("\"total_vehicles\":").append(r.get("total_vehicles")).append(",")
                        .append("\"total_revenue\":").append(r.get("total_revenue")).append(",")
                        .append("\"total_penalty\":").append(r.get("total_penalty")).append(",")
                        .append("\"avg_duration_mins\":").append(r.get("avg_duration_mins")).append(",")
                        .append("\"max_duration_mins\":").append(r.get("max_duration_mins"))
                        .append("}");
            }

            sb.append("]");
        }

        sb.append("}}");
        return sb.toString();
    }

    // ── HELPER ───────────────────────────────────────────

    private static String quote(String value) {
        if (value == null)
            return "null";
        return "\"" + value.replace("\"", "\\\"") + "\"";
    }
}