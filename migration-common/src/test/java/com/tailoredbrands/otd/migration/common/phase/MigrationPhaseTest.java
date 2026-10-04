package com.tailoredbrands.otd.migration.common.phase;

import com.tailoredbrands.otd.migration.common.jms.JmsSettings;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MigrationPhaseTest {

    @Test
    void phaseTableMatchesArchitectureSection7() {
        assertThat(MigrationPhase.LEGACY_ONLY.bridgesLegacyToPubSub()).isFalse();
        assertThat(MigrationPhase.LEGACY_ONLY.bridgesPubSubToLegacy()).isFalse();
        assertThat(MigrationPhase.SHADOW.bridgesLegacyToPubSub()).isTrue();
        assertThat(MigrationPhase.SHADOW.bridgesPubSubToLegacy()).isFalse();
        assertThat(MigrationPhase.DUAL_RUN.bridgesLegacyToPubSub()).isTrue();
        assertThat(MigrationPhase.DUAL_RUN.bridgesPubSubToLegacy()).isTrue();
        assertThat(MigrationPhase.PUBSUB_PRIMARY.bridgesLegacyToPubSub()).isTrue();
        assertThat(MigrationPhase.PUBSUB_PRIMARY.bridgesPubSubToLegacy()).isTrue();
        assertThat(MigrationPhase.CUTOVER.bridgesLegacyToPubSub()).isFalse();
        assertThat(MigrationPhase.CUTOVER.bridgesPubSubToLegacy()).isFalse();
        assertThat(MigrationPhase.CUTOVER.number()).isEqualTo(4);
    }

    @Test
    void parsesLeniently() {
        assertThat(MigrationPhase.parse("DUAL_RUN")).isEqualTo(MigrationPhase.DUAL_RUN);
        assertThat(MigrationPhase.parse(" dual-run ")).isEqualTo(MigrationPhase.DUAL_RUN);
        assertThat(MigrationPhase.parse("\"SHADOW\"")).isEqualTo(MigrationPhase.SHADOW);
        assertThat(MigrationPhase.parse("phase-3")).isEqualTo(MigrationPhase.PUBSUB_PRIMARY);
        assertThat(MigrationPhase.parse("0")).isEqualTo(MigrationPhase.LEGACY_ONLY);
        assertThatThrownBy(() -> MigrationPhase.parse("BANANA")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MigrationPhase.parse(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sourceNotifiesListenersOnChangeOnly() {
        MigrationPhaseSource source = new MigrationPhaseSource(MigrationPhase.LEGACY_ONLY);
        List<MigrationPhase> seen = new ArrayList<>();
        source.onChange(seen::add);
        assertThat(seen).containsExactly(MigrationPhase.LEGACY_ONLY); // immediate call with current

        assertThat(source.update(MigrationPhase.LEGACY_ONLY, "test")).isFalse();
        assertThat(source.update(MigrationPhase.SHADOW, "test")).isTrue();
        assertThat(source.update(MigrationPhase.DUAL_RUN, "test")).isTrue();
        assertThat(source.update(null, "test")).isFalse();

        assertThat(seen).containsExactly(MigrationPhase.LEGACY_ONLY, MigrationPhase.SHADOW, MigrationPhase.DUAL_RUN);
        assertThat(source.current()).isEqualTo(MigrationPhase.DUAL_RUN);
        assertThat(source.lastOrigin()).isEqualTo("test");
    }

    @Test
    void jmsProviderParsing() {
        assertThat(JmsSettings.Provider.parse("artemis")).isEqualTo(JmsSettings.Provider.ARTEMIS);
        assertThat(JmsSettings.Provider.parse("IBMMQ")).isEqualTo(JmsSettings.Provider.IBMMQ);
        assertThat(JmsSettings.Provider.parse("ibm-mq")).isEqualTo(JmsSettings.Provider.IBMMQ);
        assertThat(JmsSettings.Provider.parse("ems")).isEqualTo(JmsSettings.Provider.EMS);
        assertThat(JmsSettings.Provider.parse(null)).isEqualTo(JmsSettings.Provider.ARTEMIS);
        assertThatThrownBy(() -> JmsSettings.Provider.parse("rabbit")).isInstanceOf(IllegalArgumentException.class);
    }
}
