-- =============================================================================
-- SIMPLIFIED ANALYTICS AGGREGATIONS FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION refresh_analytics_aggregations()
RETURNS VOID AS $$
DECLARE
    total_patients INTEGER := 0;
    high_risk_count INTEGER := 0;
    avg_risk NUMERIC := 0;
BEGIN
    RAISE NOTICE 'Starting simplified analytics aggregations refresh';
    
    -- Get basic statistics from gold layer
    SELECT 
        COUNT(*),
        COUNT(*) FILTER (WHERE high_risk_flag = TRUE),
        AVG(risk_score)
    INTO total_patients, high_risk_count, avg_risk
    FROM gold.diabetes_features;
    
    RAISE NOTICE 'Analytics Summary:';
    RAISE NOTICE '- Total Patients: %', total_patients;
    RAISE NOTICE '- High Risk Patients: %', high_risk_count;
    RAISE NOTICE '- Average Risk Score: %', ROUND(avg_risk::NUMERIC, 2);
    RAISE NOTICE '- High Risk Rate: %%%', ROUND((high_risk_count::NUMERIC / NULLIF(total_patients, 0) * 100), 2);
    
    -- Log success
    RAISE NOTICE 'Analytics aggregations refresh completed successfully';
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in analytics aggregations refresh: %', SQLERRM;
    RAISE;
END;
$$ LANGUAGE plpgsql;
