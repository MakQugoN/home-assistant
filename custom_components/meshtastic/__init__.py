# SPDX-FileCopyrightText: 2024-2025 Pascal Brogle @broglep
#
# SPDX-License-Identifier: MIT

"""
Custom integration to integrate Meshtastic with Home Assistant.

For more details about this integration, please refer to
https://github.com/meshtastic/home-assistant
"""

from __future__ import annotations

import asyncio
import base64
import datetime
from collections import defaultdict
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, cast

from homeassistant import config_entries
from homeassistant.components.logbook import DOMAIN as LOGBOOK_DOMAIN
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import (
    CONF_HOST,
    CONF_PORT,
    Platform,
)
from homeassistant.core import callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers import config_validation as cv
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.device_registry import DeviceConnectionCollisionError
from homeassistant.helpers.entity_component import EntityComponent
from homeassistant.helpers.typing import UNDEFINED, ConfigType
from homeassistant.loader import async_get_loaded_integration

from . import frontend, meshtastic_web, services
from .api import (
    ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID,
    ATTR_EVENT_MESHTASTIC_API_DATA,
    ATTR_EVENT_MESHTASTIC_API_NODE,
    EVENT_MESHTASTIC_API_NODE_UPDATED,
    MeshtasticApiClient,
)
from .const import (
    CONF_CONNECTION_TCP_HOST,
    CONF_CONNECTION_TCP_PORT,
    CONF_CONNECTION_TYPE,
    CONF_OPTION_FILTER_NODES,
    CONF_OPTION_TCP_PROXY,
    CONF_OPTION_TCP_PROXY_ENABLE,
    CONF_OPTION_TCP_PROXY_ENABLE_DEFAULT,
    CONF_OPTION_WEB_CLIENT,
    CONF_OPTION_WEB_CLIENT_ENABLE,
    CONF_OPTION_WEB_CLIENT_ENABLE_DEFAULT,
    CURRENT_CONFIG_VERSION_MAJOR,
    CURRENT_CONFIG_VERSION_MINOR,
    DOMAIN,
    LOGGER,
    ConnectionType,
)
from .coordinator import MeshtasticDataUpdateCoordinator
from .data import DATA_COMPONENT, MeshtasticConfigEntry, MeshtasticData
from .entity import (
    GatewayChannelEntity,
    GatewayDirectMessageEntity,
    GatewayEntity,
    MeshtasticEntity,
)
from .helpers import async_prune_stale_node_entities, fetch_meshtastic_hardware_names, node_identity_key
from .logbook import async_setup_message_logger
from .meshtastic_tcp import async_setup_tcp_proxy, async_unload_tcp_proxy

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, MutableMapping

    from homeassistant.core import Event, HomeAssistant, _DataT
    from homeassistant.helpers.device_registry import DeviceRegistry
    from homeassistant.helpers.entity import Entity

PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.BINARY_SENSOR,
    Platform.DEVICE_TRACKER,
    Platform.NOTIFY,
    Platform.BUTTON,
]

ENTITY_ID_FORMAT = DOMAIN + ".{}"
PLATFORM_SCHEMA = cv.PLATFORM_SCHEMA
PLATFORM_SCHEMA_BASE = cv.PLATFORM_SCHEMA_BASE
SCAN_INTERVAL = datetime.timedelta(hours=1)


_remove_listeners: MutableMapping[str, list[Callable[[], None]]] = defaultdict(list)
_last_non_filter_options: dict[str, dict[str, Any]] = {}
_filter_apply_in_progress: set[str] = set()


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    component = hass.data[DATA_COMPONENT] = EntityComponent[MeshtasticEntity](LOGGER, DOMAIN, hass, SCAN_INTERVAL)

    await component.async_setup(config)
    await services.async_setup_services(hass)

    return True


async def async_setup_meshtastic_web(hass: HomeAssistant) -> bool:
    try:
        # widoki HTTP/static paths nie da się zarejestrować dwa razy ani cofnąć —
        # ta flaga jest ustawiana raz na cały proces HA i NIGDY czyszczona
        if not hass.data[DOMAIN].config.get("meshtastic_web_views_registered", False):
            await meshtastic_web.async_setup(hass)
            hass.data[DOMAIN].config["meshtastic_web_views_registered"] = True

        # panel we frontendzie da się dodawać/usuwać normalnie — ta flaga
        # nadal odzwierciedla, czy jest aktualnie widoczny
        if not hass.data[DOMAIN].config.get("meshtastic_web_loaded", False):
            await frontend.async_register_frontend(hass)
            hass.data[DOMAIN].config["meshtastic_web_loaded"] = True
    except:  # noqa: E722
        LOGGER.warning("Failed to setup frontend", exc_info=True)
        return False
    else:
        return True


async def async_unload_meshtastic_web(hass: HomeAssistant) -> bool:
    if not hass.data[DOMAIN].config.get("meshtastic_web_loaded", False):
        return True

    try:
        await frontend.async_unregister_frontend(hass)
        hass.data[DOMAIN].config["meshtastic_web_loaded"] = False
    except:  # noqa: E722
        LOGGER.warning("Failed to unload frontend", exc_info=True)
        return False
    else:
        return True

_UNIT_MIGRATIONS: list[tuple[str, str, str]] = [
    ("environment_current", "A", "mA"),
    ("stats_heap_total_bytes", "B", "kB"),
    ("stats_heap_free_bytes", "B", "kB"),
    ("host_freemem_bytes", "B", "kB"),
    ("host_diskfree1_bytes", "B", "kB"),
    ("host_diskfree2_bytes", "B", "kB"),
    ("host_diskfree3_bytes", "B", "kB"),
]

_PRECISION_MIGRATION_KEYS: set[str] = {
    "device_voltage", "power_ch1_voltage", "power_ch1_current",
    "power_ch2_voltage", "power_ch2_current", "power_ch3_voltage", "power_ch3_current",
    "device_channel_utilization", "device_airtime", "node_snr",
    "stats_heap_total_bytes", "stats_heap_free_bytes", "stats_noise_floor",
    "environment_temperature", "environment_relative_humidity", "environment_barometric_pressure",
    "environment_gas_resistance", "environment_distance", "environment_lux", "environment_white_lux",
    "environment_ir_lux", "environment_uv_lux", "environment_wind_speed", "environment_wind_gust",
    "environment_wind_lull", "environment_voltage", "environment_current",
    "environment_rainfall1h", "environment_rainfall24h", "environment_soil_moisture",
    "environment_soil_temperature",
    "airquality_pm10_standard", "airquality_pm25_standard", "airquality_pm100_standard",
    "airquality_pm10_environmental", "airquality_pm25_environmental", "airquality_pm100_environmental",
    "airquality_particles03um", "airquality_particles05um", "airquality_particles10um",
    "airquality_particles25um", "airquality_particles50um", "airquality_particles100um",
    "host_load1", "host_load5", "host_load15",
}


def _migrate_sensor_display_options(hass: HomeAssistant, entry: MeshtasticConfigEntry) -> None:
    registry = er.async_get(hass)
    for reg_entry in er.async_entries_for_config_entry(registry, entry.entry_id):
        if reg_entry.domain != "sensor" or not reg_entry.unique_id:
            continue

        sensor_options = reg_entry.options.get("sensor", {})
        new_sensor_options = dict(sensor_options)

        for key, _old_unit, new_unit in _UNIT_MIGRATIONS:
            if reg_entry.unique_id.endswith(f"_{key}"):
                # SensorEntity reads a per-entity unit override from
                # options["sensor"]["unit_of_measurement"], not from the top-level
                # RegistryEntry.unit_of_measurement field — setting the latter (via
                # async_update_entity(unit_of_measurement=...)) is stored but never
                # actually read for display, so it silently does nothing.
                new_sensor_options["unit_of_measurement"] = new_unit
                break

        for key in _PRECISION_MIGRATION_KEYS:
            if reg_entry.unique_id.endswith(f"_{key}"):
                if "display_precision" not in sensor_options and "suggested_display_precision" not in sensor_options:
                    new_sensor_options["display_precision"] = 2
                    new_sensor_options["suggested_display_precision"] = 2
                break

        if new_sensor_options != sensor_options:
            try:
                registry.async_update_entity_options(reg_entry.entity_id, "sensor", new_sensor_options)
            except Exception:  # noqa: BLE001
                LOGGER.warning("Failed migrating display options for %s", reg_entry.entity_id, exc_info=True)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: MeshtasticConfigEntry,
) -> bool:
    _migrate_sensor_display_options(hass, entry)

    coordinator = MeshtasticDataUpdateCoordinator(hass=hass)
    if coordinator.config_entry is None:
        coordinator.config_entry = entry

    client = MeshtasticApiClient(entry.data, hass=hass, config_entry_id=entry.entry_id)

    try:
        await client.connect()
    except Exception as e:
        raise ConfigEntryNotReady from e

    gateway_node = await client.async_get_own_node()
    if "num" not in gateway_node:
        # connected_node_ready() can flip true slightly before the separate listener that
        # populates our own node's info catches up with the same packet stream (a real race
        # under rapid reconnects, e.g. right at startup, or right after an options-triggered
        # reload). Give it a few seconds to catch up before giving up — resolves the common
        # case within this same setup attempt instead of always bouncing out to HA's slower
        # ConfigEntryNotReady retry/backoff.
        for _ in range(10):
            await asyncio.sleep(0.5)
            gateway_node = await client.async_get_own_node()
            if "num" in gateway_node:
                break
        else:
            try:
                await client.disconnect()
            except Exception:  # noqa: BLE001
                pass
            msg = "Connected, but the gateway node's own info is not available yet"
            raise ConfigEntryNotReady(msg)

    entry.runtime_data = MeshtasticData(
        client=client,
        integration=async_get_loaded_integration(hass, entry.domain),
        coordinator=coordinator,
        gateway_node=gateway_node,
    )

    # Start the TCP proxy as early as possible — it only needs entry.runtime_data
    # (set just above), not the rest of this function. Other integrations connecting
    # through it (e.g. a second HA integration sharing this node) shouldn't have to
    # wait for platform/entity/device/service setup to finish before the port opens.
    if entry.options.get(CONF_OPTION_TCP_PROXY, {}).get(
        CONF_OPTION_TCP_PROXY_ENABLE, CONF_OPTION_TCP_PROXY_ENABLE_DEFAULT
    ):
        await async_setup_tcp_proxy(hass, entry)

    if entry.state == ConfigEntryState.SETUP_IN_PROGRESS:
        await coordinator.async_config_entry_first_refresh()

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))

    await _setup_meshtastic_devices(hass, entry, client)
    await _setup_meshtastic_entities(hass, entry, client)

    cancel_device_name_sync = _setup_device_name_sync(hass, entry)
    _remove_listeners[entry.entry_id].append(cancel_device_name_sync)

    await services.async_register_gateway(hass, entry)

    # listeners
    cancel_message_logger = await async_setup_message_logger(hass, entry)
    _remove_listeners[entry.entry_id].append(cancel_message_logger)

    if entry.options.get(CONF_OPTION_WEB_CLIENT, {}).get(
        CONF_OPTION_WEB_CLIENT_ENABLE, CONF_OPTION_WEB_CLIENT_ENABLE_DEFAULT
    ):
        await async_setup_meshtastic_web(hass)
        await meshtastic_web.async_setup_web_proxy_server(hass, entry)

    _last_non_filter_options[entry.entry_id] = _non_filter_options(entry)

    return True


async def _setup_meshtastic_devices(
    hass: HomeAssistant, entry: MeshtasticConfigEntry, client: MeshtasticApiClient
) -> None:
    gateway_node = await client.async_get_own_node()
    coordinator = entry.runtime_data.coordinator
    nodes = coordinator.data or {}
    device_registry = dr.async_get(hass)
    filter_nodes = entry.options.get(CONF_OPTION_FILTER_NODES, [])
    filter_node_nums = [el["id"] for el in filter_nodes]
    configured_identity_keys = {el["identity_key"] for el in filter_nodes if el.get("identity_key")}
    device_hardware_names = await fetch_meshtastic_hardware_names(hass)
    # pass 1: create every device first, without via_device — guarantees every
    # node already exists in the registry (and has recorded connections) before
    # pass 2 tries to link via_device or compute the closest gateway
    for node_id, node in nodes.items():
        await _setup_meshtastic_device(
            client, device_hardware_names, device_registry, entry, gateway_node, node, node_id,
            ignore_via_device=True,
        )

    # pass 2: now link via_device / closest-gateway, all targets already exist
    for node_id, node in nodes.items():
        await _setup_meshtastic_device(
            client, device_hardware_names, device_registry, entry, gateway_node, node, node_id
        )

    # remove devices for nodes no longer in the filter, based on what is
    # actually registered for this config entry — not just nodes that
    # happen to appear in coordinator.data right now (a node that's
    # offline or hasn't reported yet this session would otherwise be
    # silently skipped and left as an orphaned device forever). A device
    # is only removed if NONE of the raw node numbers it has ever been
    # seen under, AND NONE of its identity-key identifiers, are in the
    # filter — checked separately because a raw number never parses as
    # an identity key (pk_.../num_...) and vice versa, so a device with
    # only one kind of identifier must still be checked against the
    # matching kind of filter entry.
    for device in dr.async_entries_for_config_entry(device_registry, entry.entry_id):
        legacy_node_ids = _legacy_node_ids_from_device(device)
        identity_keys = _identity_keys_from_device(device)
        still_tracked = not legacy_node_ids.isdisjoint(filter_node_nums) or not identity_keys.isdisjoint(
            configured_identity_keys
        )
        if (legacy_node_ids or identity_keys) and not still_tracked:
            await _remove_meshtastic_device(device_registry, entry, device)

    return gateway_node


def _legacy_node_ids_from_device(device: dr.DeviceEntry) -> set[int]:
    """Return every raw node number ever recorded as an identifier for this device."""
    node_ids = set()
    for domain, identifier in device.identifiers:
        if domain != DOMAIN:
            continue
        try:
            node_ids.add(int(identifier))
        except ValueError:
            continue
    return node_ids


def _identity_keys_from_device(device: dr.DeviceEntry) -> set[str]:
    """Return every identity-key style identifier (pk_.../num_...) recorded for this device."""
    return {identifier for domain, identifier in device.identifiers if domain == DOMAIN and not identifier.isdigit()}


async def async_remove_entry(hass: HomeAssistant, entry: MeshtasticConfigEntry) -> None:
    """
    Clean up entities/devices when the user fully removes this integration.

    This only runs on an explicit removal, never on a reload/unload — the identity-key
    persistence that keeps entities matched across restarts and reloads (the whole point
    of node_identity_key) is untouched by this. Some of our entities (gateway/channel)
    are registered through a bespoke path (_add_entities_for_entry) rather than the
    standard per-platform AddEntitiesCallback flow, which appears to bypass HA's usual
    automatic entity/device registry cleanup on removal — so it's done explicitly here.
    """
    entity_registry = er.async_get(hass)
    for reg_entry in er.async_entries_for_config_entry(entity_registry, entry.entry_id):
        try:
            entity_registry.async_remove(reg_entry.entity_id)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to remove entity %s during integration removal", reg_entry.entity_id, exc_info=True)

    device_registry = dr.async_get(hass)
    for device in dr.async_entries_for_config_entry(device_registry, entry.entry_id):
        try:
            if device.config_entries == {entry.entry_id}:
                device_registry.async_remove_device(device.id)
            else:
                device_registry.async_update_device(device.id, remove_config_entry_id=entry.entry_id)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to remove device %s during integration removal", device.id, exc_info=True)


async def async_remove_config_entry_device(
    hass: HomeAssistant,  # noqa: ARG001
    config_entry: MeshtasticConfigEntry,  # noqa: ARG001
    device_entry: dr.DeviceEntry,  # noqa: ARG001
) -> bool:
    """
    Always allow manually deleting a device from its device page.

    Poprzednio blokowało to kasowanie, gdy identyfikatory urządzenia
    pokrywały się z listą śledzonych węzłów — miało chronić przed
    przypadkowym skasowaniem żywego węzła. W praktyce ta ochrona potrafi
    utknąć na stałe: identyfikatory narastają na urządzeniu z czasem (zob.
    poprawka MAC-only-match w 1.0.10), a urządzenie sklejone jeszcze przed
    tą poprawką nigdy nie spełni tego warunku, nawet po usunięciu z filtra.
    Skasowanie węzła wciąż faktycznie śledzonego jest nieszkodliwe i
    samonaprawcze — _setup_meshtastic_devices() odtworzy je od nowa,
    czysto, przy najbliższym odświeżeniu.
    """
    return True

def _setup_device_name_sync(hass: HomeAssistant, entry: MeshtasticConfigEntry) -> Callable[[], None]:
    @callback
    def _api_node_updated(event: Event[_DataT]) -> None:
        event_data = event.data
        if event_data.get(ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID) != entry.entry_id:
            return

        node_id = event_data.get(ATTR_EVENT_MESHTASTIC_API_NODE)
        node_info = event_data.get(ATTR_EVENT_MESHTASTIC_API_DATA) or {}
        new_name = node_info.get("user", {}).get("longName")
        if node_id is None or not new_name:
            return

        device_registry = dr.async_get(hass)
        device = device_registry.async_get_device_by_identifier((DOMAIN, str(node_id)), entry.entry_id)
        if device is not None and device.name != new_name:
            device_registry.async_update_device(device.id, name=new_name)

    return hass.bus.async_listen(EVENT_MESHTASTIC_API_NODE_UPDATED, _api_node_updated)


async def _remove_meshtastic_device(
    device_registry: DeviceRegistry, entry: MeshtasticConfigEntry, device: dr.DeviceEntry
) -> None:
    # only clean up devices if they are exclusively from us
    if device.config_entries == {entry.entry_id}:
        device_registry.async_remove_device(device.id)
    else:
        device_registry.async_update_device(device.id, remove_config_entry_id=entry.entry_id)


async def _setup_meshtastic_device(  # noqa: PLR0913
    client: MeshtasticApiClient,
    device_hardware_names: Mapping[str, str],
    device_registry: DeviceRegistry,
    entry: MeshtasticConfigEntry,
    gateway_node: Mapping[str, Any],
    node: Mapping[str, Any],
    node_id: int,
    *,
    ignore_via_device: bool = False,
) -> None:
    gateway_node_id = cast("int", gateway_node["num"])
    identity_key = node_identity_key(node_id, node)
    mac_address = base64.b64decode(node["user"]["macaddr"]).hex(":") if "macaddr" in node["user"] else None
    connections = set()
    if mac_address:
        connections.add((dr.CONNECTION_NETWORK_MAC, mac_address))
    hops_away = node.get("hopsAway", 99)
    snr = node.get("snr", 0)
    # look up any device we already know for this node — by identity first
    # (survives a node-number change), falling back to the raw number for
    # devices created by an older version of the integration, and finally
    # to the radio's MAC address — the one identifier that survives both a
    # node-number change *and* a node reporting a PKI public key for the
    # first time (e.g. right after a firmware update that enables it),
    # since in that case the old device has neither the new identity_key
    # nor the new node number recorded yet
    existing_device = device_registry.async_get_device_by_identifier((DOMAIN, identity_key), entry.entry_id)
    existing_device_is_same_identity = existing_device is not None
    if existing_device is None:
        existing_device = device_registry.async_get_device_by_identifier((DOMAIN, str(node_id)), entry.entry_id)
        existing_device_is_same_identity = existing_device is not None
    if existing_device is None and mac_address:
        # MAC-only match: ten sam fizyczny sprzęt, ale NIE wiadomo czy to ta sama
        # logiczna tożsamość węzła (np. po regeneracji ID/klucza na tym samym
        # radiu). Nadal przydatne niżej do via_device/connections, ale nie wolno
        # z niego dziedziczyć starych identyfikatorów — inaczej zregenerowany
        # węzeł zlewa się na stałe z poprzednikiem i nigdy nie da się go usunąć.
        existing_device = device_registry.async_get_device_by_connection(
            (dr.CONNECTION_NETWORK_MAC, mac_address), entry.entry_id
         )
        existing_device_is_same_identity = False
    via_device = None
    if existing_device is not None and existing_device.config_entries != {entry.entry_id}:
        # get other meshtastic connections

        connection_parts = [
            tuple(v.split("/"))
            for k, v in existing_device.connections
            if k == DOMAIN and not v.startswith(f"{gateway_node_id}/")
        ]
        meshtastic_connections = [
            (int(source), int(target), int(hops), float(snr)) for source, target, hops, snr in connection_parts
        ]
        if node_id == gateway_node_id:
            # add ourselves with highest prio so we don't get another via device
            meshtastic_connections.append((gateway_node_id, node_id, -1, 999))
        else:
            meshtastic_connections.append((gateway_node_id, node_id, hops_away, snr))
        try:
            sorted_connections = sorted(meshtastic_connections, key=lambda x: (x[2], -x[3]))
            closest_gateway = sorted_connections[0][0]
            via_device = (DOMAIN, str(closest_gateway))
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to find closest gateway", exc_info=True)
    else:
        via_device = (DOMAIN, str(gateway_node_id)) if gateway_node_id != node_id else None

    # remove via_device when it is set to ourself, or during the first pass where
    # every device is created before any via_device links are resolved (see
    # _setup_meshtastic_devices — avoids a HA device-registry race where via_device
    # points at a node not yet created in this run)
    if (via_device is not None and int(via_device[1]) == node_id) or (gateway_node_id == node_id) or ignore_via_device:
        via_device = None

    if existing_device:
        connections.update(existing_device.connections)

    # remove our own entry
    connections = {
        (k, v) for k, v in connections if k != DOMAIN or (k == DOMAIN and not v.startswith(f"{gateway_node_id}/"))
    }

    # add our own entry with updated data
    if gateway_node_id != node_id:
        connections.add((DOMAIN, f"{gateway_node_id}/{node_id}/{hops_away}/{snr}"))

    # identifiers: the stable identity key, plus every raw node number this
    # device has ever been seen under (including the current one) —
    # accumulated so that (a) an install upgrading from a version that only
    # knew raw numbers merges into the same device instead of duplicating
    # it, and (b) a future node-number change is still recognised as the
    # same device.
    identifiers = {(DOMAIN, identity_key), (DOMAIN, str(node_id))}
    if existing_device is not None and existing_device_is_same_identity:
        identifiers |= {(d, i) for d, i in existing_device.identifiers if d == DOMAIN}

    via_device_id = None
    if via_device is not None:
        via_device_entry = device_registry.async_get_device_by_identifier(via_device, entry.entry_id)
        via_device_id = via_device_entry.id if via_device_entry else None

    d = device_registry.async_get_or_create(
        config_entry_id=entry.entry_id,
        identifiers=identifiers,
        name=node["user"]["longName"],
        model=device_hardware_names.get(node["user"]["hwModel"], None),
        model_id=str(node["user"]["hwModel"]),
        serial_number=node["user"]["id"],
        via_device_id=via_device_id,
        sw_version=client.metadata.get("firmwareVersion")
        if gateway_node["num"] == node_id and client.metadata
        else None,
    )
    try:
        device_registry.async_update_device(
            d.id,
            new_connections=connections,
            via_device_id=None if via_device is None else UNDEFINED,
        )
    except DeviceConnectionCollisionError as e:
        LOGGER.debug("Conflict with other device connections, only using meshtastic connections. %s", e)
        own_connections = {(k, v) for k, v in connections if k == DOMAIN}
        device_registry.async_update_device(
            d.id,
            new_connections=own_connections,
            via_device_id=None if via_device is None else UNDEFINED,
        )


async def _setup_meshtastic_entities(
    hass: HomeAssistant, entry: MeshtasticConfigEntry, client: MeshtasticApiClient
) -> None:
    gateway_node = await client.async_get_own_node()
    local_config = await client.async_get_node_local_config()
    module_config = await client.async_get_node_module_config()

    gateway_node_entity = GatewayEntity(
        config_entry_id=entry.entry_id,
        node=gateway_node["num"],
        long_name=gateway_node["user"]["longName"],
        short_name=gateway_node["user"]["shortName"],
        local_config=local_config,
        module_config=module_config,
    )
    has_logbook = LOGBOOK_DOMAIN in hass.config.all_components
    gateway_direct_message = GatewayDirectMessageEntity(
        config_entry_id=entry.entry_id,
        gateway_node=gateway_node["num"],
        gateway_entity=gateway_node_entity,
        has_logbook=has_logbook,
    )

    await _add_entities_for_entry(hass, [gateway_node_entity, gateway_direct_message], entry)
    channels = await client.async_get_channels()
    channel_entities = [
        GatewayChannelEntity(
            config_entry_id=entry.entry_id,
            gateway_node=gateway_node["num"],
            gateway_entity=gateway_node_entity,
            index=channel["index"],
            name=channel["settings"]["name"],
            primary=channel["role"] == "PRIMARY",
            secondary=channel["role"] == "SECONDARY",
            settings=channel["settings"],
            has_logbook=has_logbook,
        )
        for channel in channels
        if channel["role"] != "DISABLED"
    ]
    await _add_entities_for_entry(hass, channel_entities, entry)


async def async_unload_entry(
    hass: HomeAssistant,
    entry: MeshtasticConfigEntry,
) -> bool:
    # ensure that we disconnect first to prevent later issues with duplicate connection in case of errors
    try:
        if entry.runtime_data and entry.runtime_data.client:
            await entry.runtime_data.client.disconnect()
    except:  # noqa: E722
        LOGGER.warning("Failed to disconnect client during unload of entry", exc_info=True)

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        for entity in [
            e for e in hass.data[DATA_COMPONENT].entities if e.registry_entry.config_entry_id == entry.entry_id
        ]:
            await hass.data[DATA_COMPONENT].async_remove_entity(entity.entity_id)

        await services.async_unregister_gateway(hass, entry)

        for remove_listener in _remove_listeners.pop(entry.entry_id, []):
            remove_listener()

        _last_non_filter_options.pop(entry.entry_id, None)

        active_entries = hass.config_entries.async_entries(DOMAIN, include_ignore=False, include_disabled=False)
        any_web_client_enabled = any(
            e.options.get(CONF_OPTION_WEB_CLIENT, {}).get(
                CONF_OPTION_WEB_CLIENT_ENABLE, CONF_OPTION_WEB_CLIENT_ENABLE_DEFAULT
            )
            for e in active_entries
        )

        if not any_web_client_enabled:
            await async_unload_meshtastic_web(hass)

        await meshtastic_web.async_unload_web_proxy_server(hass, entry)
        await async_unload_tcp_proxy(hass, entry)

    return unload_ok


_reload_lock = asyncio.Lock()


async def async_reload_entry(
    hass: HomeAssistant,
    entry: MeshtasticConfigEntry,
) -> None:
    async with _reload_lock:
        token = None
        if config_entries.current_entry.get() is None:
            token = config_entries.current_entry.set(entry)
        try:
            await async_unload_entry(hass, entry)
            await async_setup_entry(hass, entry)
        finally:
            if token:
                config_entries.current_entry.reset(token)


def _non_filter_options(entry: MeshtasticConfigEntry) -> dict[str, Any]:
    return {k: v for k, v in entry.options.items() if k != CONF_OPTION_FILTER_NODES}


async def _async_apply_node_filter_change(hass: HomeAssistant, entry: MeshtasticConfigEntry) -> None:
    """
    Apply a change to the tracked node filter live, without reconnecting or
    reloading the whole config entry.

    Newly added nodes get their entities created automatically once
    coordinator.data is refreshed, via the listener already registered in
    helpers.setup_platform_entry() for every node-scoped platform. This
    only needs to additionally (re)build devices for the new/changed set
    of nodes and prune entities/devices for nodes that dropped out of the
    filter — both of those already exist as idempotent, re-runnable
    functions used at normal setup time.
    """
    async with _reload_lock:
        coordinator = entry.runtime_data.coordinator
        await coordinator.async_request_refresh()
        await _setup_meshtastic_devices(hass, entry, entry.runtime_data.client)
        await async_prune_stale_node_entities(hass, entry)


async def _async_options_updated(hass: HomeAssistant, entry: MeshtasticConfigEntry) -> None:
    """
    Update listener for the config entry's options.

    If only the tracked node filter changed (nothing that affects the
    connection itself, like host/port or the TCP proxy/web client
    settings), apply it live instead of doing a full unload+setup reload —
    avoids disconnecting the gateway connection and briefly marking every
    entity unavailable just because a node was added or removed. Any other
    kind of options change still gets a full reload, same as before.
    """
    non_filter_options = _non_filter_options(entry)
    previous = _last_non_filter_options.get(entry.entry_id)
    _last_non_filter_options[entry.entry_id] = non_filter_options

    can_apply_live = (
        previous is not None and previous == non_filter_options and entry.state == ConfigEntryState.LOADED
    )

    if can_apply_live:
        if entry.entry_id in _filter_apply_in_progress:
            # A filter-apply is already running for this entry. The most
            # likely reason we got called again is that run's own
            # coordinator refresh self-healing a migrated/deduped node
            # number back into options (see _async_update_data) — that
            # in-progress run will already pick up the fully-resolved
            # state once it gets to rebuilding devices/entities, so
            # there is nothing extra to do here. Without this guard, a
            # migration detected during the live-apply path would
            # recursively re-enter this listener while still running.
            return
        _filter_apply_in_progress.add(entry.entry_id)
        try:
            await _async_apply_node_filter_change(hass, entry)
            return
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to apply node filter change live, falling back to a full reload", exc_info=True)
        finally:
            _filter_apply_in_progress.discard(entry.entry_id)

    await async_reload_entry(hass, entry)


async def async_migrate_entry(hass: HomeAssistant, config_entry: MeshtasticConfigEntry) -> bool:
    LOGGER.debug("Migrating configuration from version %s.%s", config_entry.version, config_entry.minor_version)

    if config_entry.version > CURRENT_CONFIG_VERSION_MAJOR:
        # This means the user has downgraded from a future version
        return False

    if config_entry.version == 1:
        new_data = {**config_entry.data}
        if config_entry.minor_version < 2:  # noqa: PLR2004
            new_data.update(
                {
                    CONF_CONNECTION_TYPE: ConnectionType.TCP.value,
                    CONF_CONNECTION_TCP_HOST: new_data.pop(CONF_HOST),
                    CONF_CONNECTION_TCP_PORT: new_data.pop(CONF_PORT),
                }
            )

        hass.config_entries.async_update_entry(
            config_entry,
            data=new_data,
            minor_version=CURRENT_CONFIG_VERSION_MINOR,
            version=CURRENT_CONFIG_VERSION_MAJOR,
        )

    LOGGER.debug(
        "Migration to configuration version %s.%s successful", config_entry.version, config_entry.minor_version
    )

    return True


async def _add_entities_for_entry(hass: HomeAssistant, entities: list[Entity], entry: MeshtasticConfigEntry) -> None:
    entity_registry = er.async_get(hass)
    device_registry = dr.async_get(hass)

    await hass.data[DATA_COMPONENT].async_add_entities(entities)
    # attach entities to config entry (as async_add_entities does not support apply config_entry_id from entities)
    for e in entities:
        device_id = UNDEFINED
        identifiers = getattr(e, "_device_identifiers", None)
        if identifiers:
            device = device_registry.async_get_device_by_identifier(next(iter(identifiers)), entry.entry_id)
            if device:
                device_id = device.id
        try:
            entity_registry.async_update_entity(e.entity_id, config_entry_id=entry.entry_id, device_id=device_id)
        except:  # noqa: E722
            LOGGER.warning("Failed to update entity %s", e, exc_info=True)
