package com.parking.servlet;

import com.parking.util.JsonUtil;

import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;

@WebServlet("/error")
public class ErrorServlet extends HttpServlet {

    @Override
    protected void service(HttpServletRequest req,
            HttpServletResponse res) throws IOException {

        res.setContentType("application/json");
        res.setCharacterEncoding("UTF-8");

        // Tomcat puts the real status code here
        Integer statusCode = (Integer) req.getAttribute(
                "jakarta.servlet.error.status_code");
        String errorMessage = (String) req.getAttribute(
                "jakarta.servlet.error.message");
        String requestUri = (String) req.getAttribute(
                "jakarta.servlet.error.request_uri");

        if (statusCode == null)
            statusCode = 500;

        res.setStatus(statusCode);

        switch (statusCode) {
            case 400:
                res.getWriter().write(JsonUtil.error("BAD_REQUEST",
                        "Invalid request."));
                break;
            case 401:
                res.getWriter().write(JsonUtil.error("UNAUTHORIZED",
                        "You are not logged in."));
                break;
            case 403:
                res.getWriter().write(JsonUtil.error("FORBIDDEN",
                        "You do not have permission to access this."));
                break;
            case 404:
                res.getWriter().write(JsonUtil.error("NOT_FOUND",
                        "Endpoint not found: " +
                                (requestUri != null ? requestUri : "unknown")));
                break;
            case 503:
                res.getWriter().write(JsonUtil.error("POOL_TIMEOUT",
                        "Server is busy. Please try again."));
                break;
            default:
                res.getWriter().write(JsonUtil.error("SERVER_ERROR",
                        "An unexpected server error occurred."));
                break;
        }
    }
}