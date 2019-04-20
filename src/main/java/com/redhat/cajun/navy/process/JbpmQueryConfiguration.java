package com.redhat.cajun.navy.process;

import org.jbpm.kie.services.impl.query.SqlQueryDefinition;
import org.jbpm.services.api.query.QueryService;
import org.jbpm.services.api.query.model.QueryDefinition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JbpmQueryConfiguration {

    @Autowired
    private QueryService queryService;

    @Bean
    public QueryDefinition processSignalQuery() {
        QueryDefinition processSignalQuery = new SqlQueryDefinition("signalsByProcessInstance", "source");
        processSignalQuery.setExpression("select * from eventtypes");
        queryService.registerQuery(processSignalQuery);
        return processSignalQuery;
    }

}
