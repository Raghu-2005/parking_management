package com.parking.servlet;

import com.parking.util.JsonUtil;

import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;
import java.sql.SQLException;

public abstract class BaseServlet extends HttpServlet {

    // ── EVERY SERVLET IMPLEMENTS THIS ────────────────────
    // subclasses use doGet/doPost/doPut/doDelete normally

    // ── MASTER SERVICE — wraps all requests ──────────────
    @Override
    protected void service(HttpServletRequest req,
            HttpServletResponse res) throws IOException {

        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        try {
            super.service(req, res);

        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;

            if (cause instanceof SQLException) {
                SQLException se = (SQLException) cause;
                int status = JsonUtil.resolveStatus(se);
                String code = JsonUtil.resolveCode(se);
                res.setStatus(status);
                res.getWriter().write(
                        JsonUtil.error(code, friendlyMessage(code, se)));
                log("[BaseServlet] SQLException — " + code + ": " + se.getMessage());

            } else if (cause instanceof IllegalArgumentException) {
                res.setStatus(400);
                res.getWriter().write(
                        JsonUtil.error("BAD_REQUEST", cause.getMessage()));

            } else if (cause instanceof SecurityException) {
                res.setStatus(403);
                res.getWriter().write(
                        JsonUtil.error("FORBIDDEN", cause.getMessage()));

            } else {
                res.setStatus(500);
                res.getWriter().write(
                        JsonUtil.error("SERVER_ERROR",
                                "An unexpected error occurred."));
                log("[BaseServlet] Unexpected: " + cause.getMessage());
            }
        }
    }
    // ── SESSION HELPER ───────────────────────────────────

    // Call this at the start of any protected endpoint
    protected String requireSession(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        HttpSession session = req.getSession(false);
        if (session == null || session.getAttribute("username") == null) {
            res.setStatus(401);
            res.getWriter().write(JsonUtil.error("UNAUTHORIZED",
                    "You are not logged in. Please login first."));
            return null;
        }
        return (String) session.getAttribute("username");
    }

    // ── ROLE HELPER ──────────────────────────────────────

    // Call this to enforce admin-only endpoints
    protected boolean requireAdmin(HttpServletRequest req,
            HttpServletResponse res) throws IOException {
        HttpSession session = req.getSession(false);
        if (session == null) {
            res.setStatus(401);
            res.getWriter().write(JsonUtil.error("UNAUTHORIZED",
                    "You are not logged in."));
            return false;
        }
        String role = (String) session.getAttribute("role");
        if (!"admin".equals(role)) {
            res.setStatus(403);
            res.getWriter().write(JsonUtil.error("FORBIDDEN",
                    "Admin access required."));
            return false;
        }
        return true;
    }

    // ── FRIENDLY MESSAGES ────────────────────────────────

    private String friendlyMessage(String code, Exception e) {
        switch (code) {
            case "POOL_TIMEOUT":
                return "Server is busy. Please try again in a moment.";
            case "SLOT_OCCUPIED":
                return "This slot is currently occupied by a vehicle.";
            case "ALREADY_PARKED":
                return "This vehicle is already parked.";
            case "FLOOR_BLOCKED":
                return "This floor is currently blocked.";
            case "NOT_FOUND":
                return "The requested resource was not found.";
            case "BAD_REQUEST":
                return e.getMessage();
            default:
                return "A database error occurred. Please try again.";
        }
    }
}