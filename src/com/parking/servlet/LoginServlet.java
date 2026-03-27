package com.parking.servlet;

import com.parking.dao.ParkingDAO;
import com.parking.util.JsonUtil;

import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;

@WebServlet({ "/login", "/register", "/logout" })
public class LoginServlet extends BaseServlet {

    private ParkingDAO dao;

    @Override
    public void init() throws ServletException {
        dao = (ParkingDAO) getServletContext()
                .getAttribute("parkingDAO");

        if (dao == null) {
            throw new ServletException(
                    "ParkingDAO not found in context");
        }
    }

    // ── GET /login ───────────────────────────────────────
    @Override
    protected void doGet(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        HttpSession session = req.getSession(false);
        if (session != null && session.getAttribute("username") != null) {
            String username = (String) session.getAttribute("username");
            String role = (String) session.getAttribute("role");
            res.setStatus(200);
            res.getWriter().write(
                    "{\"status\":\"success\","
                            + "\"username\":\"" + username + "\","
                            + "\"role\":\"" + role + "\"}");
        } else {
            res.setStatus(401);
            res.getWriter().write(JsonUtil.error("UNAUTHORIZED",
                    "You are not logged in."));
        }
    }

    // ── POST /login /register /logout ────────────────────
    @Override
    protected void doPost(HttpServletRequest req,
            HttpServletResponse res)
            throws ServletException, IOException {

        String path = req.getServletPath();

        // handle logout
        if ("/logout".equals(path)) {
            handleLogout(req, res);
            return;
        }

        // handle register
        if ("/register".equals(path)) {
            handleRegister(req, res);
            return;
        }

        // handle login
        handleLogin(req, res);
    }

    // ── LOGIN ────────────────────────────────────────────
    private void handleLogin(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        String username = req.getParameter("username");
        String password = req.getParameter("password");

        // validate params
        if (username == null || username.isBlank() ||
                password == null || password.isBlank()) {

            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS",
                    "username and password are required"));
            return;
        }

        try {
            // fetch user from DB
            String[] user = dao.getUserByUsername(username.trim());

            if (user == null) {
                res.setStatus(401);
                res.getWriter().write(JsonUtil.error(
                        "UNAUTHORIZED",
                        "Invalid username or password"));
                return;
            }

            String dbPassword = user[1];
            String role = user[2];
            String status = user[3];

            // check account status
            if ("pending".equals(status)) {
                res.setStatus(403);
                res.getWriter().write(JsonUtil.error(
                        "PENDING",
                        "Your account is awaiting admin approval"));
                return;
            }
            if ("rejected".equals(status)) {
                res.setStatus(403);
                res.getWriter().write(JsonUtil.error(
                        "REJECTED",
                        "Your account registration was rejected"));
                return;
            }

            // check password
            if (!password.equals(dbPassword)) {
                res.setStatus(401);
                res.getWriter().write(JsonUtil.error(
                        "UNAUTHORIZED",
                        "Invalid username or password"));
                return;
            }

            // create session
            HttpSession session = req.getSession(true);
            session.setAttribute("username", username.trim());
            session.setAttribute("role", role);
            session.setMaxInactiveInterval(30 * 60); // 30 minutes

            res.setStatus(200);
            res.getWriter().write(
                    "{"
                            + "\"status\":\"success\","
                            + "\"message\":\"Login successful\","
                            + "\"role\":\"" + role + "\""
                            + "}");

            System.out.println(
                    "[Login] User logged in: " + username
                            + " role: " + role);

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "DB_ERROR",
                    "Login failed: " + e.getMessage()));
        }
    }

    // ── REGISTER ─────────────────────────────────────────
    private void handleRegister(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        String username = req.getParameter("username");
        String password = req.getParameter("password");
        String role = req.getParameter("role");

        // validate
        if (username == null || username.isBlank() ||
                password == null || password.isBlank() ||
                role == null || role.isBlank()) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_PARAMS",
                    "username, password and role are required"));
            return;
        }

        // only allow admin or moderator roles
        if (!"admin".equals(role) && !"moderator".equals(role)) {
            res.setStatus(400);
            res.getWriter().write(JsonUtil.error(
                    "INVALID_ROLE",
                    "Role must be admin or moderator"));
            return;
        }

        try {
            // check if username already exists
            String[] existing = dao.getUserByUsername(username.trim());
            if (existing != null) {
                res.setStatus(409);
                res.getWriter().write(JsonUtil.error(
                        "USERNAME_TAKEN",
                        "Username already exists"));
                return;
            }

            dao.registerUser(username.trim(), password, role);

            res.setStatus(200);
            res.getWriter().write(
                    "{\"status\":\"success\","
                            + "\"message\":\"Registration submitted. "
                            + "Please wait for admin approval.\"}");

            System.out.println(
                    "[Register] New registration: " + username
                            + " role: " + role);

        } catch (Exception e) {
            res.setStatus(500);
            res.getWriter().write(JsonUtil.error(
                    "DB_ERROR",
                    "Registration failed: " + e.getMessage()));
        }
    }

    // ── LOGOUT ───────────────────────────────────────────
    private void handleLogout(HttpServletRequest req,
            HttpServletResponse res)
            throws IOException {

        HttpSession session = req.getSession(false);

        if (session != null) {
            String username = (String) session
                    .getAttribute("username");
            session.invalidate();
            System.out.println(
                    "[Logout] User logged out: " + username);
        }

        res.setStatus(200);
        res.getWriter().write(JsonUtil.success(
                "Logged out successfully"));
    }

    // ── HELPER — check if logged in ──────────────────────
    public static boolean isAuthorized(HttpServletRequest req) {
        HttpSession session = req.getSession(false);
        return session != null
                && session.getAttribute("username") != null;
    }

    // ── HELPER — check if admin ──────────────────────────
    public static boolean isAdmin(HttpServletRequest req) {
        HttpSession session = req.getSession(false);
        if (session == null)
            return false;
        return "admin".equals(session.getAttribute("role"));
    }

    // ── HELPER — check if moderator ─────────────────────
    public static boolean isModerator(HttpServletRequest req) {
        HttpSession session = req.getSession(false);
        if (session == null)
            return false;
        return "moderator".equals(session.getAttribute("role"));
    }

    // ── HELPER — check if admin or moderator ─────────────
    public static boolean isAdminOrModerator(HttpServletRequest req) {
        HttpSession session = req.getSession(false);
        if (session == null)
            return false;
        String role = (String) session.getAttribute("role");
        return "admin".equals(role) || "moderator".equals(role);
    }

    // ── HELPER — get role ────────────────────────────────
    public static String getRole(HttpServletRequest req) {
        HttpSession session = req.getSession(false);
        if (session == null)
            return null;
        return (String) session.getAttribute("role");
    }
}