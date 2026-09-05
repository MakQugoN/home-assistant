# SPDX-FileCopyrightText: 2024-2025 Pascal Brogle @broglep
#
# SPDX-License-Identifier: MIT

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import timedelta
from functools import wraps
from typing import TYPE_CHECKING, Any

from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .api import (
    ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID,
    ATTR_EVENT_MESHTASTIC_API_DATA,
    ATTR_EVENT_MESHTASTIC_API_NODE,
    EVENT_MESHTASTIC_API_NODE_UPDATED,
    EVENT_MESHTASTIC_API_POSITION,
    EVENT_MESHTASTIC_API_TELEMETRY,
    EventMeshtasticApiTelemetryType,
    MeshtasticApiClientError,
)
from .const import CONF_OPTION_FILTER_NODES, DOMAIN, LOGGER
from .helpers import node_identity_key

EVENT_MESHTASTIC_NODE_IDENTITY_MIGRATED = f"{DOMAIN}_node_identity_migrated"

ATTR_EVENT_MESHTASTIC_IDENTITY_CONFIG_ENTRY_ID = "config_entry_id"
ATTR_EVENT_MESHTASTIC_IDENTITY_KEY = "identity_key"
ATTR_EVENT_MESHTASTIC_IDENTITY_OLD_NODE = "old_node_id"
ATTR_EVENT_MESHTASTIC_IDENTITY_NEW_NODE = "new_node_id"

if TYPE_CHECKING:
    from collections.abc import Mapping

    from homeassistant.core import Event, HomeAssistant, _DataT

    from .data import MeshtasticConfigEntry


def meshtastic_api_event_callback(f):  # noqa: ANN001, ANN201
    @wraps(f)
    async def wrapper(self: MeshtasticDataUpdateCoordinator, event: Event[_DataT]):  # noqa: ANN202
        try:
            if self.config_entry is None:
                return None

            event_data = deepcopy(event.data)
            config_entry_id = event_data.pop(ATTR_EVENT_MESHTASTIC_API_CONFIG_ENTRY_ID, None)
            if config_entry_id != self.config_entry.entry_id:
                return None

            if not self.data:
                self._logger.debug("Received event but coordinator is not yet initialized")
                return None

            node_id = event_data.get(ATTR_EVENT_MESHTASTIC_API_NODE, None)
            if node_id is None:
                return None

            if node_id not in self.data:
                await self._try_recover_tracked_node(node_id)

            if node_id not in self.data:
                self._logger.debug("Node %d not in coordinator data", node_id)
                return None

            data = event_data.get(ATTR_EVENT_MESHTASTIC_API_DATA, None)
            if data is None:
                self._logger.debug("Event did not contain data")
                return None

            additional_event_data = {
                k: v
                for k, v in event_data.items()
                if k not in [ATTR_EVENT_MESHTASTIC_API_NODE, ATTR_EVENT_MESHTASTIC_API_DATA]
            }

            return await f(self, node_id, data, **additional_event_data)
        except:  # noqa: E722
            self._logger.warning("Failed to handle meshtastic api event", exc_info=True)

    return wrapper


class MeshtasticDataUpdateCoordinator(DataUpdateCoordinator):
    config_entry: MeshtasticConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
    ) -> None:
        super().__init__(
            hass=hass,
            logger=LOGGER,
            name=DOMAIN,
            update_interval=timedelta(hours=1),
        )
        self._logger = LOGGER.getChild(self.__class__.__name__)
        self._tracked_identity_by_num: dict[int, str] = {}
        self._pending_removals: set[int] = set()
        self._node_id_migrations: dict[int, int] = {}
        self._remove_event_listeners = []
        self._remove_event_listeners.append(
            hass.bus.async_listen(EVENT_MESHTASTIC_API_NODE_UPDATED, self._api_node_updated)
        )
        self._remove_event_listeners.append(hass.bus.async_listen(EVENT_MESHTASTIC_API_TELEMETRY, self._api_telemetry))
        self._remove_event_listeners.append(hass.bus.async_listen(EVENT_MESHTASTIC_API_POSITION, self._api_position))

    async def async_shutdown(self) -> None:
        await super().async_shutdown()

        for remove_listener in self._remove_event_listeners:
            try:
                remove_listener()
            except:  # noqa: E722
                self._logger.debug("Could not remove event listeners", exc_info=True)

    async def _try_recover_tracked_node(self, node_id: int) -> None:
        """
        Re-add a tracked node to coordinator.data if it fell out during an
        unlucky hourly refresh (e.g. one that overlapped a reconnect). Without
        this, live events for the node keep getting silently dropped until
        the next scheduled refresh, up to update_interval later.
        """
        if self.config_entry is None or self.config_entry.runtime_data is None:
            return

        filter_nodes = self.config_entry.options.get(CONF_OPTION_FILTER_NODES, [])
        filter_node_nums = {el["id"] for el in filter_nodes}
        if node_id not in filter_node_nums:
            return

        try:
            node_infos = await asyncio.wait_for(
                self.config_entry.runtime_data.client.async_get_all_nodes(), timeout=10
            )
        except (MeshtasticApiClientError, TimeoutError):
            return

        if node_id not in node_infos:
            return

        data = deepcopy(self.data)
        data[node_id] = deepcopy(node_infos[node_id])
        self.async_set_updated_data(data)
        self._logger.info("Recovered tracked node %d that had fallen out of coordinator data", node_id)

    _REMOVE_NODE_TIMEOUT_SECONDS = 20

    async def _attempt_remove_node(self, node_num: int, *, quick_retries: int = 0) -> bool:
        """
        Best-effort removal of a node from the gateway's on-device node
        database. On failure the node is added to _pending_removals so
        every future coordinator refresh keeps retrying it, instead of
        losing the removal for good the moment it lands during a brief
        connection hiccup. Pass quick_retries when an immediate answer
        matters (e.g. right after the user removes a node in the options
        flow) — it retries a few times a few seconds apart before falling
        back to the passive retry-on-next-refresh path.

        The underlying admin-message call has no timeout of its own — if
        the gateway never acks the remove request, it would otherwise
        hang forever, blocking this entire coordinator refresh (including
        the very first one during setup, which can then take down the
        whole integration's startup with it — seen in practice as Home
        Assistant's own bootstrap-stage-2 timeout forcibly cancelling
        setup after several minutes). Bounded here so a single
        unresponsive removal can only ever cost a few seconds.
        """
        if self.config_entry is None or self.config_entry.runtime_data is None:
            self._pending_removals.add(node_num)
            return False

        client = self.config_entry.runtime_data.client
        for attempt in range(quick_retries + 1):
            try:
                removed = await asyncio.wait_for(
                    client.async_remove_node(node_num), timeout=self._REMOVE_NODE_TIMEOUT_SECONDS
                )
            except Exception:  # noqa: BLE001
                removed = False
                self._logger.debug(
                    "Failed to remove stale node %d from the on-device node database (attempt %d/%d)",
                    node_num,
                    attempt + 1,
                    quick_retries + 1,
                    exc_info=True,
                )
            if removed:
                self._logger.info("Removed stale node %d from the on-device node database", node_num)
                self._pending_removals.discard(node_num)
                return True
            if attempt < quick_retries:
                await asyncio.sleep(3)

        self._logger.warning(
            "Could not remove stale node %d from the on-device node database yet — will keep retrying", node_num
        )
        self._pending_removals.add(node_num)
        return False

    async def async_request_node_removal(self, node_num: int) -> None:
        """
        Called when a node is dropped from the tracked filter list (e.g.
        via the options flow) — tries removal right away with a few quick
        retries, instead of waiting for the coordinator's normal (hourly)
        refresh cycle to get to it.
        """
        await self._attempt_remove_node(node_num, quick_retries=3)

    @meshtastic_api_event_callback
    async def _api_node_updated(self, node_id: int, node_data: Mapping[str, Any], **kwargs) -> None:  # noqa: ANN003, ARG002
        if self.data[node_id] != node_data:
            data = deepcopy(self.data)
            data[node_id].update(node_data)
            self.async_set_updated_data(data)

    @meshtastic_api_event_callback
    async def _api_telemetry(
        self,
        node_id: int,
        data: Mapping[str, Any],
        *,
        telemetry_type: EventMeshtasticApiTelemetryType,
        **kwargs,  # noqa: ANN003, ARG002
    ) -> None:
        if telemetry_type == EventMeshtasticApiTelemetryType.DEVICE_METRICS:
            metric_type = "deviceMetrics"
        elif telemetry_type == EventMeshtasticApiTelemetryType.LOCAL_STATS:
            metric_type = "localStats"
        elif telemetry_type == EventMeshtasticApiTelemetryType.LOCAL_STATS_EXTENDED:
            metric_type = "localStatsExtended"
        elif telemetry_type == EventMeshtasticApiTelemetryType.POWER_METRICS:
            metric_type = "powerMetrics"
        elif telemetry_type == EventMeshtasticApiTelemetryType.ENVIRONMENT_METRICS:
            metric_type = "environmentMetrics"
        elif telemetry_type == EventMeshtasticApiTelemetryType.HOST_METRICS:
            metric_type = "hostMetrics"
        else:
            self._logger.warning("Unsupported telemetry type %s", telemetry_type)
            return

        new_metrics = data
        existing_metrics = self.data[node_id].get(metric_type, None)
        if existing_metrics == new_metrics:
            self._logger.debug("Received telemetry identical to existing metrics, ignoring event")
            return

        data = deepcopy(self.data)
        data[node_id][metric_type] = new_metrics
        self.async_set_updated_data(data)

    @meshtastic_api_event_callback
    async def _api_position(
        self,
        node_id: int,
        data: Mapping[str, Any],
        **kwargs,  # noqa: ANN003, ARG002
    ) -> None:
        new_position = data
        existing_position = self.data[node_id].get("position", {})
        if existing_position == new_position:
            self._logger.debug("Received position identical to existing position, ignoring event")
            return

        data = deepcopy(self.data)
        data[node_id]["position"] = new_position
        self.async_set_updated_data(data)

    async def _node_updated(self, event: Event) -> None:
        if self.config_entry is None:
            return

        event_data = deepcopy(event.data)
        config_entry_id = event_data.pop("config_entry_id", None)
        if config_entry_id != self.config_entry.entry_id:
            return

        if not self.data:
            self._logger.debug("Received updated metrics but coordinator data is empty")
            return

        node_id = event_data.get("num", None)
        if node_id is None or node_id not in self.data:
            # oczekiwane dla każdego węzła spoza filtra — nie logujemy, bo zalewa log
            return

        if self.data[node_id] != event_data:
            data = deepcopy(self.data)
            data[node_id] = event_data
            self.async_set_updated_data(data)

    async def _async_update_data(self) -> Any:
        if self.config_entry is None or self.config_entry.runtime_data is None:
            self._logger.warning("Update data requested but config entry is empty")
            return None

        try:
            node_infos = await self.config_entry.runtime_data.client.async_get_all_nodes()
        except MeshtasticApiClientError as exception:
            raise UpdateFailed(exception) from exception

        if self._pending_removals:
            for node_num in list(self._pending_removals):
                await self._attempt_remove_node(node_num)

        filter_nodes = self.config_entry.options.get(CONF_OPTION_FILTER_NODES, [])
        filter_node_nums = [el["id"] for el in filter_nodes]
        configured_identity_keys = {el["identity_key"] for el in filter_nodes if el.get("identity_key")}

        # Node numbers are normally stable, but the firmware regenerates a
        # new random num if it detects a collision with another node on
        # the mesh. Build an identity_key -> live num index from
        # everything currently visible on the mesh so a tracked node
        # whose num just changed can still be found via the identity
        # (public key) it had the last time we saw it.
        #
        # The gateway's own node table can briefly hold both an old and a
        # new entry for the same identity after a "warm reconnect" (see
        # the dedup step below) — a plain dict comprehension would pick
        # whichever happens to land last in iteration order, which is not
        # necessarily the current one. Keep whichever has the more recent
        # lastHeard instead, same tie-breaker the dedup step already uses.
        live_identity_index: dict[str, int] = {}
        live_identity_last_heard: dict[str, int] = {}
        for node_num, node_info in node_infos.items():
            identity_key = node_identity_key(node_num, node_info)
            last_heard = node_info.get("lastHeard") or 0
            if identity_key not in live_identity_index or last_heard > live_identity_last_heard[identity_key]:
                live_identity_index[identity_key] = node_num
                live_identity_last_heard[identity_key] = last_heard

        resolved_node_nums = set()
        updated_filter_nodes = []
        filter_changed = False
        for el_config in filter_nodes:
            tracked_num = el_config["id"]
            if tracked_num in node_infos:
                resolved_node_nums.add(tracked_num)
                # keep the stored identity in sync with live data — fills
                # in a missing identity_key, and upgrades a raw-number
                # fallback to the node's real public key as soon as it
                # reports one (e.g. after a firmware update that enables
                # PKI for the first time). Without this, a fallback
                # identity is a dead end the moment the num it names is
                # no longer live: it can never be resolved back.
                current_identity_key = node_identity_key(tracked_num, node_infos[tracked_num])
                updated_el = el_config
                if el_config.get("identity_key") != current_identity_key:
                    updated_el = {**el_config, "identity_key": current_identity_key}
                    filter_changed = True
                updated_filter_nodes.append(updated_el)
                continue

            # not currently live under its configured number — try to
            # follow it via identity, preferring the identity stored in
            # the filter config itself (durable across HA restarts) and
            # falling back to what we've observed so far this session
            known_identity_key = el_config.get("identity_key") or self._tracked_identity_by_num.get(tracked_num)
            new_num = live_identity_index.get(known_identity_key) if known_identity_key else None
            updated_el = el_config
            if new_num is not None and new_num != tracked_num:
                self._logger.info(
                    "Node %d appears to have a new node number %d (unchanged identity %s)",
                    tracked_num,
                    new_num,
                    known_identity_key,
                )
                resolved_node_nums.add(new_num)
                self._node_id_migrations[tracked_num] = new_num
                self.hass.bus.async_fire(
                    EVENT_MESHTASTIC_NODE_IDENTITY_MIGRATED,
                    {
                        ATTR_EVENT_MESHTASTIC_IDENTITY_CONFIG_ENTRY_ID: self.config_entry.entry_id,
                        ATTR_EVENT_MESHTASTIC_IDENTITY_KEY: known_identity_key,
                        ATTR_EVENT_MESHTASTIC_IDENTITY_OLD_NODE: tracked_num,
                        ATTR_EVENT_MESHTASTIC_IDENTITY_NEW_NODE: new_num,
                    },
                )
                # also ask the gateway to drop the now-stale old number from its own
                # on-device node database, so it stops being reported as "live" in
                # future polls — this is what previously let a stale old number win
                # the dedup below over the node's real, current number. Best-effort:
                # never let a failure here (radio asleep/busy) break the data update.
                await self._attempt_remove_node(tracked_num)
                # self-heal the configured number so the filter (and the
                # options UI) reflect where this node actually lives now
                updated_el = {**el_config, "id": new_num, "identity_key": known_identity_key}
                filter_changed = True
            # else: node is genuinely offline, or its identity was never
            # captured (no public key, and its old num is no longer live
            # under any known identity) — nothing we can do automatically;
            # it stays out of self.data until it's seen again.
            updated_filter_nodes.append(updated_el)

        # de-duplicate filter entries that ended up pointing at the same
        # physical node. This happens when an old entry (e.g. left over
        # from before identity-based tracking, or from an earlier node
        # number migration) was never removed and a separate entry for
        # the node's current number exists too — both resolve to the same
        # identity_key, so entities get built twice under the same
        # unique_id and the entity registry rejects the second copy.
        # Keep whichever entry is currently live; drop the rest.
        seen_by_identity: dict[str, dict] = {}
        deduped_filter_nodes = []
        for el_config in updated_filter_nodes:
            identity_key = el_config.get("identity_key")
            if not identity_key:
                deduped_filter_nodes.append(el_config)
                continue

            existing = seen_by_identity.get(identity_key)
            if existing is None:
                seen_by_identity[identity_key] = el_config
                deduped_filter_nodes.append(el_config)
                continue

            filter_changed = True
            existing_live = existing["id"] in node_infos
            current_live = el_config["id"] in node_infos
            # the node database keeps old entries around after a reconnect (see the
            # "warm reconnect" note above), so both can show up as technically
            # "live" even when one of them is actually stale. When that happens,
            # break the tie using whichever one has reported in more recently —
            # a safety net for entries that predate the on-device cleanup above.
            if existing_live and current_live:
                existing_last_heard = node_infos[existing["id"]].get("lastHeard") or 0
                current_last_heard = node_infos[el_config["id"]].get("lastHeard") or 0
                current_live = current_last_heard > existing_last_heard
                existing_live = not current_live
            if current_live and not existing_live:
                self._logger.info(
                    "Dropping duplicate filter entry for node %d (superseded by live node %d, identity %s)",
                    existing["id"],
                    el_config["id"],
                    identity_key,
                )
                deduped_filter_nodes.remove(existing)
                deduped_filter_nodes.append(el_config)
                seen_by_identity[identity_key] = el_config
                resolved_node_nums.discard(existing["id"])
                resolved_node_nums.add(el_config["id"])
                self._node_id_migrations[existing["id"]] = el_config["id"]
                await self._attempt_remove_node(existing["id"])
            else:
                self._logger.info(
                    "Dropping duplicate filter entry for node %d (identity %s already tracked via node %d)",
                    el_config["id"],
                    identity_key,
                    existing["id"],
                )
                resolved_node_nums.discard(el_config["id"])
                self._node_id_migrations[el_config["id"]] = existing["id"]
                await self._attempt_remove_node(el_config["id"])
        updated_filter_nodes = deduped_filter_nodes

        if filter_changed:
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                options={**self.config_entry.options, CONF_OPTION_FILTER_NODES: updated_filter_nodes},
            )

        new_data = {node_num: deepcopy(node_infos[node_num]) for node_num in resolved_node_nums}
        # merge (not replace) so a node that's simply offline this poll
        # doesn't lose its last-known identity. Drop entries only for
        # numbers that have genuinely fallen out of the filter — checked
        # by identity first (so a migrated node's new num isn't pruned
        # just because it doesn't literally match the configured raw
        # number), falling back to the raw number for filter entries
        # that don't have a stored identity_key yet.
        for node_num, node_info in new_data.items():
            self._tracked_identity_by_num[node_num] = node_identity_key(node_num, node_info)
        self._tracked_identity_by_num = {
            node_num: identity_key
            for node_num, identity_key in self._tracked_identity_by_num.items()
            if node_num in filter_node_nums or identity_key in configured_identity_keys
        }

        return new_data

    def resolve_migrated_node_id(self, node_id: int) -> int:
        """
        Follow this node number through any migrations/dedups we've
        recorded, to whatever number it currently lives under.

        Unlike resolve_node_id(), this doesn't depend on identity_key
        string matching at all — it's a direct old-number -> new-number
        lineage, so it still works even if an entity's cached identity_key
        (captured once, at creation) has since gone stale, e.g. because
        the node only exchanged its PKI public key after the entity was
        first created. Bounded against cycles/self-loops.
        """
        seen = {node_id}
        while node_id in self._node_id_migrations:
            node_id = self._node_id_migrations[node_id]
            if node_id in seen:
                break
            seen.add(node_id)
        return node_id

    def resolve_node_id(self, identity_key: str) -> int | None:
        """Return the current node number for a known identity key, if any."""
        if not self.data:
            return None
        for node_num, node_data in self.data.items():
            if node_identity_key(node_num, node_data) == identity_key:
                return node_num
        return None

    def identity_key_for(self, node_id: int) -> str:
        """
        Return the best-known identity key for a tracked node number.

        Prefers a live/tracked identity, but only if it's the real
        (public-key-based) one — a live snapshot that hasn't caught up
        yet (e.g. right after a reconnect) must not downgrade a node to
        its raw-number fallback if we already know better. Falls back
        to the identity persisted in the filter config (durable across
        HA restarts), and only then to the raw-number format
        node_identity_key() itself uses for a node we've never seen.
        """
        live_identity = None
        if self.data and node_id in self.data:
            live_identity = node_identity_key(node_id, self.data[node_id])
        if live_identity and not live_identity.startswith("num_"):
            return live_identity

        tracked_identity = self._tracked_identity_by_num.get(node_id)
        if tracked_identity and not tracked_identity.startswith("num_"):
            return tracked_identity

        if self.config_entry is not None:
            for el_config in self.config_entry.options.get(CONF_OPTION_FILTER_NODES, []):
                if el_config.get("id") == node_id and el_config.get("identity_key"):
                    return el_config["identity_key"]

        return live_identity or tracked_identity or node_identity_key(node_id, None)
