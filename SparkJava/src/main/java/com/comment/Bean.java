package com.comment;

import java.io.Serializable;

public class Bean implements Serializable {
    private String ota_bu_behavior_da_id  ;
    private String da_id;
    private String event_id;
    private String event_value;
    private String creator;
    private String created_date;
    private String modifier;
    private String last_updated_date;
    private String car_type;
    private String pt_time ;

    public Bean(String ota_bu_behavior_da_id, String da_id, String event_id, String event_value, String creator, String created_date, String modifier, String last_updated_date, String car_type, String pt_time) {
        this.ota_bu_behavior_da_id = ota_bu_behavior_da_id;
        this.da_id = da_id;
        this.event_id = event_id;
        this.event_value = event_value;
        this.creator = creator;
        this.created_date = created_date;
        this.modifier = modifier;
        this.last_updated_date = last_updated_date;
        this.car_type = car_type;
        this.pt_time = pt_time;
    }

    @Override
    public String toString() {
        return  ota_bu_behavior_da_id +
                ", " + da_id  +
                ", " + event_id  +
                ", " + event_value  +
                ", " + creator  +
                ", " + created_date  +
                ", " + modifier  +
                ", " + last_updated_date  +
                ", " + car_type  +
                ", " + pt_time ;
    }

    public String getOta_bu_behavior_da_id() {
        return ota_bu_behavior_da_id;
    }

    public void setOta_bu_behavior_da_id(String ota_bu_behavior_da_id) {
        this.ota_bu_behavior_da_id = ota_bu_behavior_da_id;
    }

    public String getDa_id() {
        return da_id;
    }

    public void setDa_id(String da_id) {
        this.da_id = da_id;
    }

    public String getEvent_id() {
        return event_id;
    }

    public void setEvent_id(String event_id) {
        this.event_id = event_id;
    }

    public String getEvent_value() {
        return event_value;
    }

    public void setEvent_value(String event_value) {
        this.event_value = event_value;
    }

    public String getCreator() {
        return creator;
    }

    public void setCreator(String creator) {
        this.creator = creator;
    }

    public String getCreated_date() {
        return created_date;
    }

    public void setCreated_date(String created_date) {
        this.created_date = created_date;
    }

    public String getModifier() {
        return modifier;
    }

    public void setModifier(String modifier) {
        this.modifier = modifier;
    }

    public String getLast_updated_date() {
        return last_updated_date;
    }

    public void setLast_updated_date(String last_updated_date) {
        this.last_updated_date = last_updated_date;
    }

    public String getCar_type() {
        return car_type;
    }

    public void setCar_type(String car_type) {
        this.car_type = car_type;
    }

    public String getPt_time() {
        return pt_time;
    }

    public void setPt_time(String pt_time) {
        this.pt_time = pt_time;
    }
}
