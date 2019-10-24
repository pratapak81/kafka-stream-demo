package com.nsc.kafkastreamdemo.controller;

import com.nsc.kafkastreamdemo.sender.MessageSender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.PathParam;

@RestController
@RequestMapping("/event")
public class EventController {

    @Autowired
    private MessageSender messageSender;

    @RequestMapping(method = RequestMethod.GET)
    public void send(@RequestParam(value = "value") Integer value) {
        messageSender.send(value);
    }

    @RequestMapping(value = "/send", method = RequestMethod.GET)
    public void send() {
        messageSender.send();
    }
}
