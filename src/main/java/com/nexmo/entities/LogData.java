package com.nexmo.entities;

import org.springframework.batch.item.validator.ValidationException;

import java.math.BigDecimal;
import java.sql.Timestamp;

/**
 * Created by Andrew Austin on 9/22/17.
 */
public class LogData {
    private Integer id;
    private String messageId;

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    private Timestamp timestamp;
    private String accountId;
    private String gatewayId;
    private String country;
    private String status;
    private BigDecimal price;
    private BigDecimal transitCost;
    private BigDecimal routeCost;

    // Derived value
    private BigDecimal cost;

    public LogData() {
    }

    public LogData(String messageId, Timestamp timestamp, String accountId, String gatewayId, String country, String status, BigDecimal price, BigDecimal transitCost, BigDecimal routeCost) {
        this.messageId = messageId;
        this.timestamp = timestamp;
        this.accountId = accountId;
        this.gatewayId = gatewayId;
        this.country = country;
        this.status = status;
        this.price = price;
        this.transitCost = transitCost;
        this.routeCost = routeCost;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }


    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getGatewayId() {
        return gatewayId;
    }

    public void setGatewayId(String gatewayId) {
        this.gatewayId = gatewayId;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    /**
     * Returns the derived cost, which is the addition of transitCost and routeCost.  If either transit or route
     * cost is unset, will return a ValidationException
     *
     * @return The calculated cost
     */
    public BigDecimal getCost() throws ValidationException {

        if (cost == null) {

            if (this.transitCost == null) {
                throw new ValidationException("routeCost is unset; cannot calculate cost");
            } else if (this.routeCost == null) {
                throw new ValidationException("routeCost is unset; cannot calculate cost");
            }

            cost = this.transitCost.add(this.routeCost);
        }

        return cost;
    }

    public BigDecimal getTransitCost() {
        return transitCost;
    }

    public void setTransitCost(BigDecimal transitCost) {
        // Changing transit cost changes derived cost - wipe it
        cost = null;
        this.transitCost = transitCost;
    }

    public BigDecimal getRouteCost() {
        return routeCost;
    }

    public void setRouteCost(BigDecimal routeCost) {
        // Changing route cost changes derived cost - wipe it
        cost = null;
        this.routeCost = routeCost;
    }
}