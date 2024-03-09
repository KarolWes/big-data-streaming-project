package com.example.bigdata;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;

public class SimpleListener implements UpdateListener {
    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement epStatement, EPRuntime epRuntime) {
        if (newEvents != null) {
            for (EventBean newEvent : newEvents) {
                System.out.println("Input Stream: " + newEvent.getUnderlying());
            }
        }
        if (oldEvents != null) {
            for (EventBean oldEvent : oldEvents) {
                System.out.println("Remove Stream: " + oldEvent.getUnderlying());
            }
        }
    }
}
