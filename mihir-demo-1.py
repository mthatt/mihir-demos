import httpx
from prefect import flow, task
from prefect.artifacts import create_markdown_artifact
from prefect.tasks import task_input_hash
from prefect.deployments import DeploymentImage
import time
@flow(log_prints=True)
def generate_complete_report(lat: float = 54.54, lon: float = 10.22):
    # random_http()
    temp = generate_temperature_report(lat, lon)
    wave = generate_wave_report(lat, lon)
    return temp, wave
    

@flow(log_prints=True)
def generate_temperature_report(lat: float, lon: float):
    time.sleep(2)
    base_url = "https://api.open-meteo.com/v1/forecast/"
    temps = call_temperature_api(lat, lon, base_url)
    forecasted_temp = get_current_temperature(temps)
    create_temperature_artifact(forecasted_temp)
    return forecasted_temp

@flow(log_prints=True)
def generate_wave_report(lat: float, lon: float):
    time.sleep(2)
    base_url = "https://marine-api.open-meteo.com/v1/marine/"
    wave = call_wave_api(lat, lon, base_url)
    wave_height = get_wave_height(wave)
    create_wave_artifact(wave_height)
    return wave_height

@task(cache_key_fn=task_input_hash, retries=3)
def call_temperature_api(lat: float, lon: float, base_url: str):
    time.sleep(2)
    temps = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="temperature_2m", temperature_unit="fahrenheit"),
    )
    return temps

@task(cache_key_fn=task_input_hash)
def get_current_temperature(temps):
    time.sleep(2)
    forecasted_temp = float(temps.json()["hourly"]["temperature_2m"][0])
    print(f"Forecasted temp F: {forecasted_temp} degrees")
    return forecasted_temp


@flow(log_prints=True)
def create_temperature_artifact(temp):
    time.sleep(2)
    print("executing artifact")
    markdown_report = create_temperature_markdown_f_string(temp)
    create_markdown_from_f_string(markdown_report, "temperature")

@task(cache_key_fn=task_input_hash, retries=3)
def create_temperature_markdown_f_string(temp):
    time.sleep(2)
    markdown_report = f"""# Weather Report
    
    ## Recent weather

    | Time        | Temperature |
    |:--------------|-------:|
    | Temp Forecast  | {temp} |
    """
    return markdown_report

@task(cache_key_fn=task_input_hash)
def call_wave_api(lat: float, lon: float, base_url: str):
    time.sleep(2)
    wave = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly="wave_height")
    )
    return wave

@task(cache_key_fn=task_input_hash)
def get_wave_height(wave):
    time.sleep(2)
    wave_height = float(wave.json()["hourly"]["wave_height"][0])
    print("\n fetching wave report \n",wave_height)
    return wave_height

@task(cache_key_fn=task_input_hash)
def create_wave_artifact(wave):
    print("executing artifact")
    markdown_report = create_wave_markdown_f_string(wave)
    create_markdown_from_f_string(markdown_report, "wave")

@task(cache_key_fn=task_input_hash)
def create_wave_markdown_f_string(wave):
    wave_markdown_report = f"""# Weather Report
    
    ## Recent weather

    | Time        | Wave Height |
    |:--------------|-------:|
    | Wave Height  | {wave} |
    """
    return wave_markdown_report

@task(cache_key_fn=task_input_hash)
def create_markdown_from_f_string(input_f_string, type):
    create_markdown_artifact(
        key= type+"-report",
        markdown=input_f_string,
        description="Very scientific " + type + "report",
    )

@task(retries = 3)
def random_http():
    # random_code = httpx.get("https://httpstat.us/Random/200,500", verify=False)
    random_code = httpx.get("https://httpstat.us/Random/200", verify=False)
    print(random_code.text)
    if random_code.status_code >= 400:
        raise Exception()


if __name__ == "__main__":
    generate_complete_report.deploy(name="mihir-demo-deployment",
                        work_pool_name="my-ecs-pool",
                        image=DeploymentImage(
                            name="prefect-flows:latest",
                            platform="linux/amd64",
                        ))
    