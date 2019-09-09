package com.itheima.mrbean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BloombergTradeBean implements Writable {

    private String tradeId;
    private String status;
    private String library;
    private String asOfDate;
    private String marketDate;
    private String tradeQuery;
    private String currency;
    private Double pv;


    public String getTradeId() {
        return tradeId;
    }

    public void setTradeId(String tradeId) {
        this.tradeId = tradeId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLibrary() {
        return library;
    }

    public void setLibrary(String library) {
        this.library = library;
    }

    public String getAsOfDate() {
        return asOfDate;
    }

    public void setAsOfDate(String asOfDate) {
        this.asOfDate = asOfDate;
    }

    public String getMarketDate() {
        return marketDate;
    }

    public void setMarketDate(String marketDate) {
        this.marketDate = marketDate;
    }

    public String getTradeQuery() {
        return tradeQuery;
    }

    public void setTradeQuery(String tradeQuery) {
        this.tradeQuery = tradeQuery;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Double getPv() {
        return pv;
    }

    public void setPv(Double pv) {
        this.pv = pv;
    }

    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append(this.tradeId);
        sb.append("\001").append(this.getStatus());
        sb.append("\001").append(this.getLibrary());
        sb.append("\001").append(this.getAsOfDate());
        sb.append("\001").append(this.getMarketDate());
        sb.append("\001").append(this.getTradeQuery());
        sb.append("\001").append(this.getPv());
        sb.append("\001").append(this.getCurrency());

        return sb.toString();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(null == tradeId?"":tradeId);
        out.writeUTF(null == status?"":status);
        out.writeUTF(null == library?"":library);
        out.writeUTF(null == asOfDate?"":asOfDate);
        out.writeUTF(null == marketDate?"":marketDate);
        out.writeUTF(null == tradeQuery?"":tradeQuery);
        out.writeUTF(null == currency?"":currency);
        out.writeDouble(pv);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tradeId = in.readUTF();
        this.status = in.readUTF();
        this.asOfDate = in.readUTF();
        this.marketDate = in.readUTF();
        this.library = in.readUTF();
        this.currency = in.readUTF();
        this.tradeQuery = in.readUTF();
        this.pv = in.readDouble();
    }
}
