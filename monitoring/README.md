## To run

Have docker installed and setup.
* Run `python monitor.py` in the background
* `docker-compose up` runs the prometheus and grafana in that order.

### Notes

* The default username for Grafana is `admin`, with the password set as an env variable in the compose file.
* For persistent storage, you use the `/monitoring/vols/prom` and `/monitoring/vols/graf` by mounting them as volumes for the app in the compose file.