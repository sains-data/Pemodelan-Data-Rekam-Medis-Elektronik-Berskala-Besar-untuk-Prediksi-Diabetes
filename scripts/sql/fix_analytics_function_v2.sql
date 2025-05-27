-- Fix refresh_analytics_aggregations function with correct column names
CREATE OR REPLACE FUNCTION refresh_analytics_aggregations()
RETURNS VOID AS $$
DECLARE
    total_patients INTEGER;
    high_risk_count INTEGER;
    avg_risk_score NUMERIC;
    high_risk_rate NUMERIC;
BEGIN
    RAISE NOTICE 'Starting simplified analytics aggregations refresh';
    
    -- Get basic statistics
    SELECT COUNT(*) INTO total_patients FROM gold.diabetes_gold;
    SELECT COUNT(*) INTO high_risk_count FROM gold.diabetes_gold WHERE health_risk_level = 'High';
    SELECT AVG(risk_score::NUMERIC) INTO avg_risk_score FROM gold.diabetes_gold WHERE risk_score IS NOT NULL;
    
    -- Calculate high risk rate with proper casting
    high_risk_rate := ROUND((high_risk_count::NUMERIC / NULLIF(total_patients, 0) * 100), 2);
    
    RAISE NOTICE 'Analytics Summary:';
    RAISE NOTICE '- Total Patients: %', total_patients;
    RAISE NOTICE '- High Risk Patients: %', high_risk_count;
    RAISE NOTICE '- Average Risk Score: %', ROUND(COALESCE(avg_risk_score, 0), 2);
    RAISE NOTICE '- High Risk Rate: %', high_risk_rate;
    
    RAISE NOTICE 'Simplified analytics aggregations refresh completed successfully';
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error in analytics aggregations refresh: %', SQLERRM;
    RAISE;
END;
$$ LANGUAGE plpgsql;
