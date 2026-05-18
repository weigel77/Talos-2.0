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

    document.addEventListener('DOMContentLoaded', function () {
        bindRefreshLinks();
    });
}());
