(function () {
    function bindRefreshLinks() {
        document.querySelectorAll('[data-refresh-market-status="true"]').forEach(function (link) {
            if (link.dataset.refreshBound === 'true') {
                return;
            }

            link.dataset.refreshBound = 'true';
            link.addEventListener('click', function () {
                try {
                    var url = new URL(link.href, window.location.origin);
                    url.searchParams.set('header_refresh', Date.now().toString());
                    link.href = url.toString();
                } catch (error) {
                    return;
                }
            });
        });
    }

    function bindTextStatusButton() {
        var button = document.getElementById('app-text-status-button');
        var feedback = document.getElementById('app-text-status-feedback');
        var script = document.currentScript || document.querySelector('script[src$="talos_header.js"]');
        var displayName = script ? (script.dataset.appDisplayName || 'Talos') : 'Talos';

        function setFeedback(text, tone) {
            if (!feedback) {
                return;
            }

            feedback.textContent = text || '';
            feedback.dataset.tone = tone || 'neutral';
        }

        if (!button || button.dataset.bound === 'true') {
            return;
        }

        button.dataset.bound = 'true';
        button.addEventListener('click', async function () {
            var originalLabel = button.textContent;

            button.disabled = true;
            button.textContent = 'Sending...';
            setFeedback('Sending ' + displayName + ' test alert...', 'neutral');

            try {
                var response = await fetch(button.dataset.textStatusUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ source: 'header-button' })
                });
                var payload = await response.json().catch(function () {
                    return {};
                });

                if (!response.ok || !payload.ok) {
                    throw new Error(payload.error || 'Unable to send Pushover test alert.');
                }

                setFeedback(payload.message || 'Pushover test alert sent.', 'success');
            } catch (error) {
                setFeedback(error.message || 'Unable to send Pushover test alert.', 'error');
            } finally {
                button.disabled = false;
                button.textContent = originalLabel;
            }
        });
    }

    document.addEventListener('DOMContentLoaded', function () {
        bindRefreshLinks();
        bindTextStatusButton();
    });
}());
