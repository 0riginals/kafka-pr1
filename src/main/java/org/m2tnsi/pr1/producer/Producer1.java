package org.m2tnsi.pr1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import javax.net.ssl.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class Producer1 extends TimerTask {
    /*
        Bug du 1er Janvier : javax.net.ssl.SSLHandshakeException: PKIX path building failed:
        sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification
        path to requested target
        -> En gros certificat self signed
        Tentative de résolution:
        https://stackoverflow.com/questions/21076179/pkix-path-building-failed-and-unable-to-find-valid-certification-path-to-requ
        (pas fonctionné)
        https://stackoverflow.com/questions/19540289/how-to-fix-the-java-security-cert-certificateexception-no-subject-alternative
        (fonctionne mais bon, c'est du copier coller temporaire j'espere tout du moins)
     */
    private static final String COVID_API_SUMMARY = "https://api.covid19api.com/summary";
    private static final String KAFKA_BROKER = "localhost:9092";
    private static final String TOPIC_1 = "Topic1";


    static {
        disableSslVerification();
    }

    private static void disableSslVerification() {
        try {
            // Create a trust manager that does not validate certificate chains
            TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(X509Certificate[] certs, String authType) {
                }
            }
            };

            // Install the all-trusting trust manager
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

            // Create all-trusting host name verifier
            HostnameVerifier allHostsValid = new HostnameVerifier() {
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            };

            // Install the all-trusting host verifier
            HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
        } catch (NoSuchAlgorithmException | KeyManagementException e) {
            e.printStackTrace();
        }
    }

    public Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "Pr1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public String getCovidApiSummary() {
        StringBuilder result = new StringBuilder();
        try {
            URL url = new URL(COVID_API_SUMMARY);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            int status = connection.getResponseCode();
            // Si on recoit bien un code 200 c'est que l'API fonctionne et nous pouvons donc lire le resultat
            // Sinon on relance notre requête après 5 secondes d'attente
            if (status == 200) {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    result.append(line);
                }
                bufferedReader.close();

                /* Ici on va venir voir si le message que l'on recoit n'est pas vide, si il l'est nous sommes
                   très probablement dans le cas de figure ou l'API se met à jour et nous n'accédons donc pas à des
                   données traitables. */
                JSONParser jsonParser = new JSONParser();
                try {
                    Object obj = jsonParser.parse(result.toString());
                    JSONObject jsonObject = (JSONObject) obj;
                    String message = String.valueOf(jsonObject.get("Message"));
                    if (!message.isBlank()) {
                        System.out.println("Le système est en cours d'actualisation");
                        TimeUnit.SECONDS.sleep(5);
                        getCovidApiSummary();
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }

            } else {
                TimeUnit.SECONDS.sleep(5);
                getCovidApiSummary();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result.toString();
    }

    @Override
    public void run() {
        Producer<String, String> pr1 = createProducer();
        String covidInfo = getCovidApiSummary();
        System.out.println("--- Réponse recu par l'API Covid 19 --- ");
        System.out.println(covidInfo);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_1, "data", covidInfo);
        pr1.send(producerRecord);
        pr1.close();
    }
}
