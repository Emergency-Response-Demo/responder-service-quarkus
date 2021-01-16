package com.redhat.erdemo.responder.rest;

import java.util.List;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.redhat.erdemo.responder.model.Responder;
import com.redhat.erdemo.responder.service.ResponderService;

@Path("/")
public class ResponderResource {

    @Inject
    ResponderService responderService;

    @GET
    @Path("/stats")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stats() {
        return Response.ok(responderService.getResponderStats()).build();
    }

    @GET
    @Path("/responder/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response responder(@PathParam("id") long id) {
        Responder responder = responderService.getResponder(id);
        if (responder == null) {
            return Response.status(Status.NOT_FOUND).build();
        } else {
            return Response.ok(responder).build();
        }
    }

    @GET
    @Path("/responder/byname/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response responderByName(@PathParam("name") String name) {
        Responder responder = responderService.getResponderByName(name);
        if (responder == null) {
            return Response.status(Status.NOT_FOUND).build();
        } else {
            return Response.ok(responder).build();
        }
    }


    @GET
    @Path("/responders/available")
    @Produces(MediaType.APPLICATION_JSON)
    public Response availableResponders(@QueryParam("limit") Optional<Integer> limit, @QueryParam("offset") Optional<Integer> offset) {
        List<Responder> responders;
        if (limit.isPresent()) {
            if (offset.isPresent()) {
                responders = responderService.availableResponders(limit.get(), offset.get());
            } else {
                responders = responderService.availableResponders(limit.get(),0);
            }
        } else {
            responders = responderService.availableResponders();
        }
        return Response.ok(responders).build();
    }

    @GET
    @Path("/responders")
    @Produces(MediaType.APPLICATION_JSON)
    public Response allResponders(@QueryParam("limit") Optional<Integer> limit, @QueryParam("offset") Optional<Integer> offset) {
        List<Responder> responders;
        if (limit.isPresent()) {
            if (offset.isPresent()) {
                responders = responderService.allResponders(limit.get(), offset.get());
            } else {
                responders = responderService.allResponders(limit.get(),0);
            }
        } else {
            responders = responderService.allResponders();
        }
        return Response.ok(responders).build();
    }

    @GET
    @Path("/responders/person")
    @Produces(MediaType.APPLICATION_JSON)
    public Response personResponders(@QueryParam("limit") Optional<Integer> limit, @QueryParam("offset") Optional<Integer> offset) {
        List<Responder> responders;
        if (limit.isPresent()) {
            if (offset.isPresent()) {
                responders = responderService.personResponders(limit.get(), offset.get());
            } else {
                responders = responderService.personResponders(limit.get(),0);
            }
        } else {
            responders = responderService.personResponders();
        }
        return Response.ok(responders).build();
    }

    @POST
    @Path("/responder")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createResponder(Responder responder) {

        responderService.createResponder(responder);
        return Response.status(Status.CREATED).build();
    }

    @POST
    @Path("/responders")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createResponders(List<Responder> responders) {

        responderService.createResponders(responders);
        return Response.status(Status.CREATED).build();
    }

    @PUT
    @Path("/responder")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response updateResponder(Responder responder) {
        responderService.updateResponder(responder);
        return Response.status(Status.NO_CONTENT).build();
    }

    @POST
    @Path("/responders/reset")
    public Response reset() {
        responderService.reset();
        return Response.ok().build();
    }

    @POST
    @Path("/responders/clear")
    public Response clear(@QueryParam("delete") Optional<String> delete) {

        if (delete.orElse("").equals("all")) {
            responderService.deleteAll();
        } else responderService.clear(delete.orElse("").equals("bots"));
        return Response.ok().build();
    }

}
